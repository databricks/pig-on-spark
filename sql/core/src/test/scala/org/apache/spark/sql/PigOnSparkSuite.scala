package org.apache.spark.sql

import org.apache.spark.sql.test.TestSQLContext
import TestSQLContext._
import org.apache.commons.io.FileUtils
import java.io.File
import org.apache.spark.sql.execution.{SparkLogicalPlan, ExistingRdd}

class PigOnSparkSuite extends QueryTest {

  val pp = new PigParser(TestSQLContext.pc)

  val filepath = "/Users/Greg/Spark/spark/sql/core/src/test/scala/org/apache/spark/sql/%s"
  val defaultSrcFile = "spork1.txt"
  val defaultDstFile = "spork2.txt"
  val delimiter = ","

  val sqlSelectQuery = "SELECT * FROM %s"
  val sqlFilterQuery = "SELECT f1, f2, f3 FROM %s WHERE (f1 > 2.0 AND  f2 > 3)"

  val defaultData =
    Seq(Seq(1.0, 1, 42), Seq(2.0, 4, 42), Seq(3.0, 9, 42), Seq(4.0, 16, 42), Seq(5.0, 25, 42))

  test("LOAD properly registers table") {
    deleteFile("spork2.txt")

    // Translate and exe; this should load spork1.txt into a table called 'a'
    val slp = pp(pigLoadStoreQuery("a", "spork1.txt", "spork2.txt"))
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
    deleteFile("spork2.txt")
    deleteFile("spork3.txt")

    // b is a table that comes from loading a known-good file
    val storeLoadPlan = pp(pigLoadStoreQuery("b", "spork1.txt", "spork2.txt"))
    // This nonsense somehow avoids the no-op problem. Hmm...
    TestSQLContext.planner.BasicOperators(storeLoadPlan).head.execute()

    //val slRdd = pql(pigLoadStoreQuery("b", "spork1.txt", "spork2.txt"))

    // c is a table that comes from loading the output of a STORE
    val loadPlan = pp(pigLoadStoreQuery("c", "spork2.txt", "spork3.txt"))
    TestSQLContext.planner.BasicOperators(loadPlan).head.execute()

    //val lRdd = pql(pigLoadStoreQuery("c", "spork2.txt", "spork3.txt"))

    val refAnswer = sql(sqlSelectQuery.format("b"))
    val testAnswer = sql(sqlSelectQuery.format("c"))

    checkAnswer(testAnswer, refAnswer.collect.toSeq)
  }

  test("FILTER basic comparisons") {
    val lt4Query = prepPigQuery("b = FILTER a BY f1 < 4.0;")
    val lt4Rdd = pql(lt4Query)
    checkAnswer(lt4Rdd, Seq(Seq(1.0, 1, 42), Seq(2.0, 4, 42), Seq(3.0, 9, 42)))

    val gt3Query = prepPigQuery("b = FILTER a BY f2 > 3;")
    val gt3Rdd = pql(gt3Query)
    checkAnswer(gt3Rdd, Seq(Seq(2.0, 4, 42), Seq(3.0, 9, 42), Seq(4.0, 16, 42), Seq(5.0, 25, 42)))
  }

  test("FILTER nested comparisons") {
    val andQuery = prepPigQuery("b = FILTER a BY (f1 < 4.0) AND (f2 > 3);")
    val andRdd = pql(andQuery)
    checkAnswer(andRdd, Seq(Seq(2.0, 4, 42), Seq(3.0, 9, 42)))

    val orQuery = prepPigQuery("b = FILTER a BY (f2 <= 3) OR (f1 >= 4.0);")
    val orRdd = pql(orQuery)
    checkAnswer(orRdd, Seq(Seq(1.0, 1, 42), Seq(4.0, 16, 42), Seq(5.0, 25, 42)))
  }

  test("FILTER with cast") {
    val castQuery = prepPigQuery("b = FILTER a BY (double) f3 == 42.0;")
    val castRdd = pql(castQuery)
    checkAnswer(castRdd, defaultData)
  }

  test("LIMIT") {
    val literalQuery = prepPigQuery("b = LIMIT a 3;")
    val literalRdd = pql(literalQuery)
    assert(literalRdd.collect().length == 3)

    val expQuery = prepPigQuery("b = LIMIT a (1+2);")
    val expRdd = pql(expQuery)
    assert(expRdd.collect().length == 3)

    val complicatedQuery = prepPigQuery("b = LIMIT a 5 - (6/3) + (2 * 3) % (-3) ;")
    val complicatedRdd = pql(complicatedQuery)
    assert(complicatedRdd.collect().length == 3)
  }

  test("DISTINCT") {
    deleteFile(defaultDstFile)
    val nonDistinctQuery = pigLoadStoreQuery("a", "spork1dup.txt", defaultDstFile)
    val nonDistinctRdd = pql(nonDistinctQuery)
    checkAnswer(nonDistinctRdd,
      defaultData ++ defaultData ++ defaultData ++ defaultData)

    val distinctQuery = prepPigQuery("b = DISTINCT a;", srcFile = "spork1dup.txt")
    val distinctRdd = pql(distinctQuery)
    checkAnswer(distinctRdd, defaultData)
  }

  /**
   * Deletes the destination file, then bookends the query with the default load command
   *  (spork1.txt => a) and store command (b => spork2.txt)
   */
  def prepPigQuery(query: String,
                   toLoad: String = "a",
                   srcFile: String = defaultSrcFile,
                   toStore: String = "b",
                   dstFile: String = defaultDstFile) = {
    deleteFile(defaultDstFile)
    pigLoadQuery(toLoad, srcFile) + query + pigStoreQuery(toStore, dstFile)
  }

  def pigLoadQuery(alias: String = "a", srcFile: String): String = {
    s"${alias} = LOAD '${filepath.format(srcFile)}'" +
      s"USING PigStorage('$delimiter') AS (f1:double, f2:int, f3:int);"
  }

  def pigStoreQuery(alias: String = "b", dstFile: String): String = {
    s"STORE ${alias} INTO '${filepath.format(dstFile)}' USING PigStorage('$delimiter');"
  }

  /**
   * Formats a Pig Latin query to load from a file, save to an alias, then store to another file
   * @param alias The alias under which the loaded table will be added to the catalog
   * @param srcFile The file from which the table will be loaded
   * @param dstFile The file to which the table will be stored
   */
  def pigLoadStoreQuery(alias: String, srcFile: String, dstFile: String): String = {
    pigLoadQuery(alias, srcFile) + pigStoreQuery(alias, dstFile)
  }

  def deleteFile(name: String) { FileUtils.deleteDirectory(new File(filepath.format(name))) }
}
