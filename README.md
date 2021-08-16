# Sensor data manipulation
Given two sets of data, sensor and session data, merge them together to obtain a charge session profile, along some basic aggregate data per profile.

## Charge session profiling
The charge session profile is an aggregation of all the data available for each charging session. It includes all the values for each metric, along with some aggregated values per phase.

Sample result:

```
+-------+----------+------+----------+----------+--------------------+------------+
|     id|charger_id|energy|start_time|  end_time|      charge_profile|session_time|
+-------+----------+------+----------+----------+--------------------+------------+
|4087906|     43312|  5116|1510337832|1510376390|{2 -> {[[15103722...|       38558|
|4440844|     83427|  9975|1618479694|1618486536|{3 -> {[[16184865...|        6842|
|4441046|     53258| 31843|1618418208|1618487178|{1 -> {[[16184832...|       68970|
|4441321|     57292|  5065|1618431217|1618488893|{1 -> {[[16184877...|       57676|
|4440490|     82509|   377|1618478927|1618484063|{2 -> {[[16184828...|        5136|
|4253987|     34577|  4146|1617028299|1617034813|{1 -> {[[16170288...|        6514|
|3733423|     61101|  7974|1611667945|1611672826|{3 -> {[[16116717...|        4881|
|4439971|     75570| 18604|1618433730|1618457106|{2 -> {[[16184459...|       23376|
|4441004|     14009|  9181|1618423224|1618487087|{3 -> {[[16184830...|       63863|
|4440720|     79603|  2666|1618469772|1618485529|{1 -> {[[16184855...|       15757|
|4440791|     54188|  9650|1618476740|1618485949|{1 -> {[[16184825...|        9209|
|4440272|     51697| 17685|1618412612|1618482807|{3 -> {[[16184828...|       70195|
|4440665|     71689| 28512|1618407084|1618485090|{2 -> {[[16184837...|       78006|
|4441188|     51062| 30554|1618419014|1618487957|{1 -> {[[16184854...|       68943|
|4441254|     67012| 10873|1618409586|1618466075|{3 -> {[[16184496...|       56489|
|4440790|     71936|  4864|1618480826|1618485888|{1 -> {[[16184837...|        5062|
|4441277|     35285| 11525|1618411358|1618488628|{3 -> {[[16184850...|       77270|
|4440478|    103068|  8883|1618476715|1618484113|{3 -> {[[16184833...|        7398|
|4440992|     51432|  1770|1618482951|1618486833|{2 -> {[[16184842...|        3882|
|4437848|     43312| 10089|1510486929|1510529891|{1 -> {[[15104942...|       42962|
+-------+----------+------+----------+----------+--------------------+------------+
only showing top 20 rows
```
Each row returned from the getProfiles method will have this schema:

```
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
```

The charge_profile has a map with each phase (1,2,3) as a key, and from it you can access all the values for that
session on the arrays, or the avg, max, min values. For example, to get the minimum temperature of the second phase
for a particular session:

```
scala> profile.where(col("id").equalTo("4087906")).select(col("charge_profile").getItem(2).getItem("min_temperature").as("temperature")).show()
+-----------+
|temperature|
+-----------+
|         38|
+-----------+
```

## Simple anomaly detection
Given a DataFrame of session profiles, return a log of cases where the variable value had a bigger than 10% difference to the previous value.

Sample result:

```
+-------+----------+--------+-----------+---------+--------------+-----------+
|     id|charger_id|phase_id|sensor_time|var_value|previous_value|    measure|
+-------+----------+--------+-----------+---------+--------------+-----------+
|4440819|     76526|       2| 1618484892|       23|            31|temperature|
|3504393|     43312|       3| 1510369537|       38|            50|temperature|
|3504393|     43312|       3| 1510369542|       50|            38|temperature|
|3504393|     43312|       3| 1510371338|       38|            56|temperature|
|3504393|     43312|       3| 1510371342|       56|            38|temperature|
|3504393|     43312|       3| 1510373138|       38|            54|temperature|
|3504393|     43312|       3| 1510373143|       54|            38|temperature|
|3504393|     43312|       3| 1510374938|       39|            49|temperature|
|3504393|     43312|       3| 1510375249|       47|            39|temperature|
|3504393|     43312|       3| 1510376738|       38|            42|temperature|
|3504393|     43312|       3| 1510398536|       57|            39|temperature|
|3504393|     43312|       3| 1510400140|       38|            57|temperature|
|3504393|     43312|       3| 1510400143|       57|            38|temperature|
|3504393|     43312|       3| 1510400253|       38|            57|temperature|
|3504393|     43312|       3| 1510400254|       57|            38|temperature|
|3504393|     43312|       3| 1510401940|       38|            57|temperature|
|3504393|     43312|       3| 1510401941|       57|            38|temperature|
|3504393|     43312|       3| 1510402053|       39|            57|temperature|
|3504393|     43312|       3| 1510402062|       57|            39|temperature|
|3504393|     43312|       3| 1510403741|       38|            54|temperature|
+-------+----------+--------+-----------+---------+--------------+-----------+
only showing top 20 rows
```

## Local run
To run locally, you need the data source files available and a working spark installation. It can be run with a command similar to this:
`spark-submit --master "local[4]" --class wb.sensors.SessionBriefingApp ./target/scala-2.12/wb_2.12-0.1.jar ~/Documents/wb/Charger_logs/charger_log_session ~/Documents/wb/Charger_logs/charger_log_status`