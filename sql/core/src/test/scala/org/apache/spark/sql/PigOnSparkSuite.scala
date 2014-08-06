package org.apache.spark.sql

import org.apache.spark.sql.test.TestSQLContext
import TestSQLContext._
import org.apache.commons.io.FileUtils
import java.io.File
import org.apache.spark.sql.execution.{SparkLogicalPlan, ExistingRdd}
import org.apache.spark.rdd.RDD

class PigOnSparkSuite extends QueryTest {

  val pp = new PigParser(TestSQLContext.pc, TestSQLContext.sparkContext)

  val filepath = "/Users/Greg/Spark/spark/sql/core/src/test/scala/org/apache/spark/sql/%s"
  val defaultSrcFile = "spork1.txt"
  val defaultDstFile = "spork2.txt"
  val delimiter = ","

  val sqlSelectQuery = "SELECT * FROM %s"
  val sqlFilterQuery = "SELECT f1, f2, f3 FROM %s WHERE (f1 > 2.0 AND  f2 > 3)"

  val defaultData: Seq[Seq[Any]] =
    Seq(Seq(1.0, 1, 42), Seq(2.0, 4, 42), Seq(3.0, 9, 42), Seq(4.0, 16, 42), Seq(5.0, 25, 42))
  val auxData =
    Seq(Seq("River", 0), Seq("Mal", 1), Seq("Zoey", 2), Seq("Jayne", 3))

  test("LOAD properly registers table") {
    deleteFile("spork2.txt")

    // This should load spork1.txt into a table called 'a'
    val rdd = pql(pigLoadStoreQuery("a", "spork1.txt", "spork2.txt"))
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
    val slRdd = pql(pigLoadStoreQuery("b", "spork1.txt", "spork2.txt"))

    // c is a table that comes from loading the output of a STORE
    val lRdd = pql(pigLoadStoreQuery("c", "spork2.txt", "spork3.txt"))

    val refAnswer = sql(sqlSelectQuery.format("b"))
    val testAnswer = sql(sqlSelectQuery.format("c"))

    checkAnswer(testAnswer, refAnswer.collect().toSeq)
    checkAnswer(slRdd, lRdd.collect().toSeq)

    deleteFile("spork2.txt")
    deleteFile("spork3.txt")
    val comboQuery =
      s"""
        |${pigLoadStoreQuery("b", "spork1.txt", "spork2.txt")}
        |${pigLoadStoreQuery("c", "spork2.txt", "spork3.txt")}
      """.stripMargin
    val comboRdd = pql(comboQuery)
    checkAnswer(comboRdd, refAnswer)
  }

  test("LOAD and STORE with no schema") {
    deleteFile()
    val noSchemaQuery =
      s"""
         |a = LOAD '${filepath.format(defaultSrcFile)}' USING PigStorage(',');
         |b = FILTER a BY $$1 > 3;
         |${pigStoreQuery()}
       """.stripMargin
    val noSchemaRdd = pql(noSchemaQuery)
    checkAnswer(noSchemaRdd,
      Seq(
        Seq("2.0", "4", "42"),
        Seq("3.0", "9", "42"),
        Seq("4.0", "16", "42"),
        Seq("5.0", "25", "42")))
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
    deleteFile()
    val nonDistinctQuery = pigLoadStoreQuery("a", "spork1dup.txt", defaultDstFile)
    val nonDistinctRdd = pql(nonDistinctQuery)
    checkAnswer(nonDistinctRdd,
      defaultData ++ defaultData ++ defaultData ++ defaultData)

    val distinctQuery = prepPigQuery("b = DISTINCT a;", srcFile = "spork1dup.txt")
    val distinctRdd = pql(distinctQuery)
    checkAnswer(distinctRdd, defaultData)
  }

  // This takes a while to run...
  /*
  test("SORT") {
    val loadQuery = s"a = LOAD '${filepath.format("sporksort.txt")}' " +
      "USING PigStorage(',') AS (f1:int, f2:int, f3: int, f4:int);"

    // Load data into table
    deleteFile()
    val loadRdd = pql(loadQuery + pigStoreQuery(alias="a"))
    loadRdd.collect()

    val perms = Seq(1,2,3,4).permutations

    for (perm <- perms) {
      val descStr = perm.map("f" + _ + " DESC").mkString(", ")
      val ascStr = perm.map("f" + _ + " ASC").mkString(", ")

      deleteFile()
      val pigDescQuery = loadQuery + "b = ORDER a BY " + descStr + ";" + pigStoreQuery()
      val sqlDescQuery = "SELECT * FROM a ORDER BY " + descStr
      val pigDescRdd = pql(pigDescQuery)
      val sqlDescRdd = sql(sqlDescQuery)
      checkAnswer(pigDescRdd, sqlDescRdd.collect().toSeq)

      deleteFile()
      val pigAscQuery = loadQuery + "b = ORDER a BY " + ascStr + ";" + pigStoreQuery()
      val sqlAscQuery = "SELECT * FROM a ORDER BY " + ascStr
      val pigAscRdd = pql(pigAscQuery)
      val sqlAscRdd = sql(sqlAscQuery)
      checkAnswer(pigAscRdd, sqlAscRdd.collect().toSeq)
    }
  }
  */

  test("CROSS") {
    deleteFile()
    val crossQuery = (pigLoadQuery()
      + s"b = LOAD '${filepath.format("sporkcross.txt")}' USING PigStorage(',') AS (f1:chararray, f2:int);"
      + "c = CROSS a, b;"
      + pigStoreQuery(alias = "c"))
    val crossRdd = pql(crossQuery)

    val crossData = defaultData.flatMap(x => auxData.map(y => x.toSeq ++ y.toSeq))
    checkAnswer(crossRdd, crossData)
  }

  test("INNER JOIN") {
    deleteFile()
    val joinQuery = (pigLoadQuery()
      + s"b = LOAD '${filepath.format("sporkcross.txt")}' USING PigStorage(',') AS (f1:chararray, f2:int);"
      + "c = JOIN a BY f1, b BY f2;"
      + pigStoreQuery(alias = "c"))
    val joinRdd = pql(joinQuery)

    val joinData = Seq(Seq(1.0,1,42,"Mal",1), Seq(2.0,4,42,"Zoey",2), Seq(3.0,9,42,"Jayne",3))
    checkAnswer(joinRdd, joinData)
  }

  test("LEFT JOIN") {
    deleteFile()
    val joinQuery = (pigLoadQuery()
      + s"b = LOAD '${filepath.format("sporkcross.txt")}' USING PigStorage(',') AS (f1:chararray, f2:int);"
      + "c = JOIN a BY f1 LEFT, b BY f2;"
      + pigStoreQuery(alias = "c"))
    val joinRdd = pql(joinQuery)

    val joinData = Seq(
      Seq(1.0,1,42,"Mal",1),
      Seq(2.0,4,42,"Zoey",2),
      Seq(3.0,9,42,"Jayne",3),
      Seq(4.0,16,42,null,null),
      Seq(5.0,25,42,null,null))
    checkAnswer(joinRdd, joinData)
  }

  test("RIGHT JOIN") {
    deleteFile()
    val joinQuery = (pigLoadQuery()
      + s"b = LOAD '${filepath.format("sporkcross.txt")}' USING PigStorage(',') AS (f1:chararray, f2:int);"
      + "c = JOIN a BY f1 RIGHT, b BY f2;"
      + pigStoreQuery(alias = "c"))
    val joinRdd = pql(joinQuery)

    val joinData = Seq(
      Seq(null,null,null,"River",0),
      Seq(1.0,1,42,"Mal",1),
      Seq(2.0,4,42,"Zoey",2),
      Seq(3.0,9,42,"Jayne",3))
    checkAnswer(joinRdd, joinData)
  }

  test("FULL JOIN") {
    deleteFile()
    val joinQuery = (pigLoadQuery()
      + s"b = LOAD '${filepath.format("sporkcross.txt")}' USING PigStorage(',') AS (f1:chararray, f2:int);"
      + "c = JOIN a BY f1 FULL, b BY f2;"
      + pigStoreQuery(alias = "c"))
    val joinRdd = pql(joinQuery)

    val joinData = Seq(
      Seq(null,null,null,"River",0),
      Seq(1.0,1,42,"Mal",1),
      Seq(2.0,4,42,"Zoey",2),
      Seq(3.0,9,42,"Jayne",3),
      Seq(4.0,16,42,null,null),
      Seq(5.0,25,42,null,null))
    checkAnswer(joinRdd, joinData)
  }

  test("INNER JOIN with more than 2 inputs") {
    deleteFile()
    val joinQuery = (pigLoadQuery()
      + s"b = LOAD '${filepath.format("sporkcross.txt")}' USING PigStorage(',') AS (f1:chararray, f2:int);"
      + pigLoadQuery(alias = "c")
      + s"d = LOAD '${filepath.format("sporkcross.txt")}' USING PigStorage(',') AS (f1:chararray, f2:int);"
      + "e = JOIN a BY f1, b BY f2, c BY f1, d BY f2;"
      + pigStoreQuery(alias = "e"))
    val joinRdd = pql(joinQuery)

    val joinData = Seq(
      Seq(1.0,1,42,"Mal",1,1.0,1,42,"Mal",1),
      Seq(2.0,4,42,"Zoey",2,2.0,4,42,"Zoey",2),
      Seq(3.0,9,42,"Jayne",3,3.0,9,42,"Jayne",3))
    checkAnswer(joinRdd, joinData)
  }

  /**
   * Deletes the destination file, then bookends the query with the given load command
   *  (default: spork1.txt => a) and store command (default: b => spork2.txt)
   */
  def prepPigQuery(query: String,
                   toLoad: String = "a",
                   srcFile: String = defaultSrcFile,
                   toStore: String = "b",
                   dstFile: String = defaultDstFile) = {
    deleteFile(dstFile)
    pigLoadQuery(toLoad, srcFile) + query + pigStoreQuery(toStore, dstFile)
  }

  def pigLoadQuery(alias: String = "a", srcFile: String = defaultSrcFile): String = {
    s"${alias} = LOAD '${filepath.format(srcFile)}'" +
      s"USING PigStorage('$delimiter') AS (f1:double, f2:int, f3:int);"
  }

  def pigStoreQuery(alias: String = "b", dstFile: String = defaultDstFile): String = {
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

  def deleteFile(name: String = defaultDstFile) { FileUtils.deleteDirectory(new File(filepath.format(name))) }
}
