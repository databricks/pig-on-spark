package org.apache.spark.sql

import org.apache.spark.sql.execution
import org.apache.spark.sql.test.TestSQLContext
import TestSQLContext._
import org.apache.spark.sql.catalyst.types._
import org.apache.commons.io.FileUtils
import java.io.File

class PigOnSparkSuite extends QueryTest {

  val pp = new PigParser(TestSQLContext)

  val filepath = "/Users/Greg/Spark/spark/sql/core/src/test/scala/org/apache/spark/sql/%s"

  val delimiter = ","

  val pigQuery = (
    s"%s = LOAD '%s' USING PigStorage('$delimiter') AS (f1:double, f2:int, f3:int);"
      + s"STORE %s INTO '%s' USING PigStorage('$delimiter');")

  val sqlSelectQuery = "SELECT * FROM %s"
  val sqlFilterQuery = "SELECT f1, f2, f3 FROM %s WHERE (f1 > 2.0 AND  f2 > 3)"

  test("LOAD properly registers table") {
    // Delete previously-created files
    FileUtils.deleteDirectory(new File(filepath.format("spork2.txt")))

    // Translate and exe; this should load spork1.txt into a table called 'a'
    val slp = pp(getPigQuery("a", "spork1.txt", "spork2.txt"))
    TestSQLContext.planner.BasicOperators(slp).head.execute()
    val testAnswer = sql(sqlFilterQuery.format("a"))
    checkAnswer(testAnswer, Seq(Seq(3.0, 9, 42), Seq(4.0, 16, 42), Seq(5.0, 25, 42)))
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

  test("LOAD-STORE-LOAD is the same as LOAD") {
    // Delete previously-created files
    FileUtils.deleteDirectory(new File(filepath.format("spork3.txt")))
    FileUtils.deleteDirectory(new File(filepath.format("spork4.txt")))

    // b is a table that comes from loading a known-good file
    val storeLoadPlan = pp(getPigQuery("b", "spork1.txt", "spork3.txt"))
    TestSQLContext.planner.BasicOperators(storeLoadPlan).head.execute()

    // c is a table that comes from loading the output of a STORE
    val loadPlan = pp(getPigQuery("c", "spork3.txt", "spork4.txt"))
    TestSQLContext.planner.BasicOperators(loadPlan).head.execute()

    val refAnswer = sql(sqlSelectQuery.format("b"))
    val testAnswer = sql(sqlSelectQuery.format("c"))

    checkAnswer(testAnswer, refAnswer.collect.toSeq)
  }

  /**
   * Formats a Pig Latin query to load from a file, save to an alias, then store to another file
   * @param alias The alias under which the loaded table will be added to the catalog
   * @param srcFile The file from which the table will be loaded
   * @param dstFile The file to which the table will be stored
   */
  def getPigQuery(alias: String, srcFile: String, dstFile: String): String = {
    pigQuery.format(alias, filepath.format(srcFile), alias, filepath.format(dstFile))
  }
}
