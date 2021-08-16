package wb.sensors

import org.apache.spark.sql.{DataFrame, SparkSession}
import Schemas.{SessionSchema, StatusSchema}

import org.apache.spark.sql.functions.avg


object SessionBriefingApp {

  def main(args: Array[String]) {
    val sessions_path = args(0) // Something like: "~/Documents/wb/Charger_logs/charger_log_session"
    val status_path = args(1) // Something like: "~/Documents/wb/Charger_logs/charger_log_status"

    val spark = SparkSession.builder.appName("Sensor profiling").getOrCreate()

    val session = spark.read.schema(SessionSchema).json(sessions_path)
    val status = spark.read.schema(StatusSchema).json(status_path)

    val profiles: DataFrame = ProfileGenerator.getProfiles(session, status)
    profiles.cache()
    println("Number of profiles:" + profiles.count())

    val changes: DataFrame = ChangesReport.bigChangesReport(profiles)
    println("Number of anomalies: " + changes.count())

    val averageCharginTime = profiles.groupBy("charger_id").agg(avg("session_time").as("avg_session_time"))
    println("Number of chargers with session time: " + averageCharginTime.count())

    spark.stop()
  }


}
