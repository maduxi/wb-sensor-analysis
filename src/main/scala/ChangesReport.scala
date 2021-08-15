package wb.sensors

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, explode, lag, lit}

object ChangesReport {

  def bigChangesReport(profile: DataFrame): DataFrame = {
    val phases = profile.select(col("id"),col("charger_id"),explode(col("charge_profile"))).cache()

    val temperatures = explodeVariable(phases,"temperature")
    val ac_current_rms = explodeVariable(phases,"ac_current_rms")
    val ac_voltage_rms = explodeVariable(phases,"ac_voltage_rms")

    val temperatureAnomalies = getAnomalies(temperatures,10).withColumn("mesure",lit("temperature"))
    val ac_current_rmsAnomalies = getAnomalies(ac_current_rms,10).withColumn("mesure",lit("ac_current_rms"))
    val ac_voltage_rmsAnomalies = getAnomalies(ac_voltage_rms,10).withColumn("mesure",lit("ac_voltage_rms"))

    temperatureAnomalies.union(ac_current_rmsAnomalies).union(ac_voltage_rmsAnomalies)
  }

  private def explodeVariable(phases: DataFrame,variable:String) = {
    phases.select(col("id"),col("charger_id"), col("key").as("phase_id"), explode(col("value").getField(variable)).as("temp")).
      select(col("id"),col("charger_id"), col("phase_id"), col("temp")(0).as("sensor_time"), col("temp")(1).as("var_value"))
  }

  def getAnomalies(measures: DataFrame, margin: Double) = {
    val window = Window.partitionBy("id","phase_id").orderBy("sensor_time")
    // After a null, we don't need to compare
    val previousValue = measures.withColumn("previous_value",lag(col("var_value"), 1).over(window)).where(col("previous_value").isNotNull)
    previousValue.withColumn("trigger",(col("previous_value")/col("var_value"))>margin).where(col("trigger"))
  }
}
