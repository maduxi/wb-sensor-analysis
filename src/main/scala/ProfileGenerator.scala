package wb.sensors

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
 *
 * Each row returned from the getProfiles method will have this schema:
 *
scala> profile.printSchema()
root
 |-- id: long (nullable = true)
 |-- charger_id: long (nullable = true)
 |-- energy: long (nullable = true)
 |-- start_time: long (nullable = true)
 |-- end_time: long (nullable = true)
 |-- charge_profile: map (nullable = true)
 |    |-- key: long
 |    |-- value: struct (valueContainsNull = true)
 |    |    |-- temperature: array (nullable = false)
 |    |    |    |-- element: array (containsNull = false)
 |    |    |    |    |-- element: long (containsNull = true)
 |    |    |-- ac_current_rms: array (nullable = false)
 |    |    |    |-- element: array (containsNull = false)
 |    |    |    |    |-- element: long (containsNull = true)
 |    |    |-- ac_voltage_rms: array (nullable = false)
 |    |    |    |-- element: array (containsNull = false)
 |    |    |    |    |-- element: long (containsNull = true)
 |    |    |-- avg_temperature: double (nullable = true)
 |    |    |-- avg_ac_current_rms: double (nullable = true)
 |    |    |-- avg_ac_voltage_rms: double (nullable = true)
 |    |    |-- max_temperature: long (nullable = true)
 |    |    |-- max_ac_current_rms: long (nullable = true)
 |    |    |-- max_ac_voltage_rms: long (nullable = true)
 |    |    |-- min_temperature: long (nullable = true)
 |    |    |-- min_ac_current_rms: long (nullable = true)
 |    |    |-- min_ac_voltage_rms: long (nullable = true)

 The charge_profile has a map with each phase (1,2,3) as a key, and from it you can access all the values for that
 session on the arrays, or the avg, max, min values. For example, to get the minimum temperature of the second phase
 for a particular session:

 scala> profile.where(col("id").equalTo("4087906")).select(col("charge_profile").getItem(2).getItem("min_temperature").as("temperature")).show()
+-----------+
|temperature|
+-----------+
|         38|
+-----------+

 */

object ProfileGenerator {

  def getProfiles(session: DataFrame, status: DataFrame): DataFrame = {
    // Remove non required columns early
    val shorterSession = session.drop("user_id", "total_cost")
    val shorterStatus = status.drop("server_timestamp", "id")
    val joined = joinSessionsAndStatus(shorterSession, shorterStatus)
    // We are doing this after the join, to avoid doing it for all the status
    val exploded = explodeByPhase(joined)
    val profiles = aggregateData(exploded)
    addSessionTime(profiles)
  }

  def explodeByPhase(joined: DataFrame): DataFrame = {
    val a = explode(col("charger_phases")).alias("phase_info") :: joined.columns.map(col).toList
    val exploded = joined.select(a: _*).drop("charger_phases")
    exploded.withColumn("phase_id", col("phase_info").getField("id"))
      .withColumn("temperature", col("phase_info").getField("temperature"))
      .withColumn("ac_current_rms", col("phase_info").getField("ac_current_rms"))
      .withColumn("ac_voltage_rms", col("phase_info").getField("ac_voltage_rms"))
      .drop("phase_info")
  }

  private def aggregateData(exploded: DataFrame): DataFrame = {
    val byPhase = exploded.groupBy("id", "charger_id", "energy", "start_time", "end_time", "phase_id").agg(
      struct(collect_list(array(col("charger_timestamp"), col("temperature"))).as("temperature"),
        collect_list(array(col("charger_timestamp"), col("ac_current_rms"))).as("ac_current_rms"),
        collect_list(array(col("charger_timestamp"), col("ac_voltage_rms"))).as("ac_voltage_rms"),
        avg("temperature").as("avg_temperature"),
        avg("ac_current_rms").as("avg_ac_current_rms"),
        avg("ac_voltage_rms").as("avg_ac_voltage_rms"),
        max("temperature").as("max_temperature"),
        max("ac_current_rms").as("max_ac_current_rms"),
        max("ac_voltage_rms").as("max_ac_voltage_rms"),
        min("temperature").as("min_temperature"),
        min("ac_current_rms").as("min_ac_current_rms"),
        min("ac_voltage_rms").as("min_ac_voltage_rms")
      ).as("phase_data")
    )
    mergePhases(byPhase)
  }

  private def mergePhases(byPhase: DataFrame) = {
    val collected = byPhase.groupBy("id", "charger_id", "energy", "start_time", "end_time").agg(
      collect_list(map(col("phase_id"), col("phase_data"))).as("charge_phase_profile")
    )
    val emptyMap = map().cast(Schemas.mapType)
    collected.withColumn("charge_profile",
      aggregate(
        col("charge_phase_profile"),
        emptyMap,
        merge = (x, y) => map_concat(x, y)
      )
    ).drop("charge_phase_profile")
  }

  private def joinSessionsAndStatus(session: DataFrame, status: DataFrame): DataFrame = {
    val condition: Column = session("charger_id") <=> status("charger_id") &&
      status("charger_timestamp").between(session("start_time"), session("end_time"))
    session.join(status, condition, "left").drop(status("charger_id"))
  }

  def addSessionTime(session: DataFrame): DataFrame = {
    session.withColumn("session_time", col("end_time") - col("start_time"))
  }
}
