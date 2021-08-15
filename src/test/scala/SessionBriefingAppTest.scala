package wb.sensors

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSuite

// We would need to cover all the other methods with tests, this is just a demo of how it could be done.
class ProfileGeneratorTest extends FunSuite with DataFrameSuiteBase {

  test("testAddSessionTime") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val input1 = sc.parallelize(List[(Long, Long)]((1616929035l, 1616929036l))).toDF("start_time", "end_time")
    val expected = sc.parallelize(List[(Long, Long, Long)]((1616929035l, 1616929036l, 1l)))
      .toDF("start_time", "end_time", "session_time")
    val result = ProfileGenerator.addSessionTime(input1)
    assertDataFrameEquals(expected, result)
  }

  test("getBriefings") {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val input1 = sc.parallelize(List[(Long, Long)]((1616929035l, 1616929036l))).toDF("start_time", "end_time")
    val expected = sc.parallelize(List[(Long, Long, Long)]((1616929035l, 1616929036l, 1l)))
      .toDF("start_time", "end_time", "session_time")
    val result = ProfileGenerator.addSessionTime(input1)
    assertDataFrameEquals(expected, result)
  }


}
