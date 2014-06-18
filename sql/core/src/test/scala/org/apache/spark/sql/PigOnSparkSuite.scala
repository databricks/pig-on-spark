package org.apache.spark.sql

import org.apache.spark.sql.test.TestSQLContext
import TestSQLContext._
import org.apache.spark.sql.catalyst.types._

class PigOnSparkSuite extends QueryTest {

  val pp = new PigParser(TestSQLContext)

  val spork1path = "/Users/Greg/Spark/spark/sql/core/src/test/scala/org/apache/spark/sql/spork1.txt"

  val delimiter = ","

  val pigQuery = (
    "a = LOAD '" + spork1path + "' USING PigStorage('" + delimiter + "') AS (f1:double, f2:int, f3:int);"
      + "b = FILTER a BY (f1 > 2.0) AND (f2 > 3);" + "STORE b INTO 'empty';")

  val sqlBaseQuery = "SELECT f1, f2, f3 FROM %s WHERE (f1 > 2.0 AND  f2 > 3)"

  test("LOAD properly registers table") {
    // Run translator; this should load spork1.txt into a table called 'a'
    val slp = pp(pigQuery)
    val testAnswer = sql(sqlBaseQuery.format("a"))
    checkAnswer(testAnswer, Seq(Seq(4.0, 4, 42), Seq(5.0, 5, 42)))
  }

  /* Minimal test to throw a java.io.NotSerializableException
  test("Triggers NotSerializableException") {
    val data = TestSQLContext.sparkContext.textFile(spork1path).map(_.split(delimiter)).map(
      r => new DataRow(r(0).toDouble, r(1).toInt, r(2).toInt))
    TestSQLContext.registerRDDAsTable(data, "aref")

    val refAnswer = sql("SELECT f1, f2, f3 FROM aref WHERE (f1 > 2.0 AND  f2 > 3)")
    refAnswer.collect.map(println)
  }
*/
}
