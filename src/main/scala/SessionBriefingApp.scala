package wb.sensors

import org.apache.spark.sql.{DataFrame, SparkSession}
import Schemas.{SessionSchema, StatusSchema}


object SessionBriefingApp {

  val sessions_path = "./Charger_logs/charger_log_session"
  val status_path = "./Charger_logs/charger_log_status"

  def main(args: Array[String]) {
    // Leaving this as local so it can run without further configuration for test
    val spark = SparkSession.builder.appName("Simple Application")
      .config("spark.master", "local")
      .getOrCreate()

    val session = spark.read.schema(SessionSchema).json(sessions_path)
    val status = spark.read.schema(StatusSchema).json(status_path)


    // Profiles has a
    val profiles: DataFrame = ProfileGenerator.getProfiles(session, status)
    profiles.show()

    val changes: DataFrame = ChangesReport.bigChangesReport(profiles)
    changes.show()

    spark.stop()
  }


}
