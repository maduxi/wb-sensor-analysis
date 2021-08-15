package wb.sensors

import org.apache.spark.sql.types._

object Schemas {
  val StatusSchema = new StructType()
    .add("id", LongType, false)
    .add("charger_id", LongType, false)
    .add("charger_phases", new ArrayType(
      new StructType()
        .add("id", LongType, false)
        .add("temperature", LongType, false)
        .add("ac_current_rms", LongType, false)
        .add("ac_voltage_rms", LongType, false)
      , false
    ))
    .add("charger_timestamp", LongType, false)
    .add("server_timestamp", TimestampType, false)

  val SessionSchema = new StructType()
    .add("id", LongType, true)
    .add("charger_id", LongType, true)
    .add("user_id", LongType, true)
    .add("energy", LongType, true)
    .add("total_cost", new DecimalType(38, 18), true)
    .add("start_time", LongType, true)
    .add("end_time", LongType, true)

  val mapType = DataTypes.createMapType(LongType, new StructType()
    .add("temperature",ArrayType(ArrayType(LongType,true),false),false)
    .add("ac_current_rms",ArrayType(ArrayType(LongType,true),false),false)
    .add("ac_voltage_rms",ArrayType(ArrayType(LongType,true),false),false)
    .add("avg_temperature",DoubleType,true)
    .add("avg_ac_current_rms",DoubleType,true)
    .add("avg_ac_voltage_rms",DoubleType,true)
    .add("max_temperature",LongType,true)
    .add("max_ac_current_rms",LongType,true)
    .add("max_ac_voltage_rms",LongType,true)
    .add("min_temperature",LongType,true)
    .add("min_ac_current_rms",LongType,true)
    .add("min_ac_voltage_rms",LongType,true))
}
