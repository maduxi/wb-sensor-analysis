package wb.sensors

import org.apache.spark.sql.{DataFrame, SparkSession}
import Schemas.{SessionSchema, StatusSchema}


object SessionBriefingApp {

  def main(args: Array[String]) {
    val sessions_path = args(0) // Something like: "~/Documents/wb/Charger_logs/charger_log_session"
    val status_path = args(1) // Something like: "~/Documents/wb/Charger_logs/charger_log_status"

    val spark = SparkSession.builder.appName("Sensor profiling").getOrCreate()

    val session = spark.read.schema(SessionSchema).json(sessions_path)
    val status = spark.read.schema(StatusSchema).json(status_path)

    val profiles: DataFrame = ProfileGenerator.getProfiles(session, status)
    profiles.cache()
    profiles.show()
    println(profiles.count())

    val changes: DataFrame = ChangesReport.bigChangesReport(profiles)
    changes.cache()
    changes.show()
    println(changes.count())

    spark.stop()
  }


}
