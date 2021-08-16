package wb.sensors

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, col, explode, lag, lit}

/*
Given a profiles DataFrame it will return a DataFrame with each time a value has a difference of more than 10%
from the previous value
 */
object ChangesReport {

  def bigChangesReport(profile: DataFrame): DataFrame = {
    val phases = profile.select(col("id"),col("charger_id"),explode(col("charge_profile"))).cache()

    val temperatures = explodeVariable(phases,"temperature")
    val ac_current_rms = explodeVariable(phases,"ac_current_rms")
    val ac_voltage_rms = explodeVariable(phases,"ac_voltage_rms")

    val acceptable_margin = 10
    val temperatureAnomalies = getAnomalies(temperatures,acceptable_margin).withColumn("measure",lit("temperature"))
    val ac_current_rmsAnomalies = getAnomalies(ac_current_rms,acceptable_margin).withColumn("measure",lit("ac_current_rms"))
    val ac_voltage_rmsAnomalies = getAnomalies(ac_voltage_rms,acceptable_margin).withColumn("measure",lit("ac_voltage_rms"))

    temperatureAnomalies.union(ac_current_rmsAnomalies).union(ac_voltage_rmsAnomalies)
  }

  private def explodeVariable(phases: DataFrame,variable:String) = {
    phases.select(col("id"),col("charger_id"), col("key").as("phase_id"), explode(col("value").getField(variable)).as("temp")).
      select(col("id"),col("charger_id"), col("phase_id"), col("temp")(0).as("sensor_time"), col("temp")(1).as("var_value"))
  }

  def getAnomalies(measures: DataFrame, margin: Double) = {
    val window = Window.partitionBy("id","phase_id").orderBy("sensor_time")
    // A null means is the first in the session
    val previousValue = measures.withColumn("previous_value",lag(col("var_value"), 1).over(window)).where(col("previous_value").isNotNull)
    previousValue.withColumn("trigger", abs(col("previous_value")/col("var_value"))> margin).where(col("trigger")).drop("trigger")
  }
}
