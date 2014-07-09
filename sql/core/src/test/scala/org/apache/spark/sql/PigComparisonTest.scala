package org.apache.spark.sql

import java.io._

import scala.collection.immutable.HashMap
import scala.util.matching.Regex
import scala.util.matching.Regex.Match

import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import org.apache.commons.io.FileUtils

import org.apache.spark.sql.Logging
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.test.TestSQLContext
import scala.sys.process.{ProcessLogger, Process}


/**
 * Allows the creations of tests that execute the same query against both Pig
 * and catalyst, comparing the results. Lots of copypasta from HiveComparisonTest.
 *
 * The "golden" results from Pig are cached in an retrieved both from the classpath and
 * [[answerCache]] to speed up testing.
 *
 * See the documentation of public vals in this class for information on how test execution can be
 * configured using system properties.
 */

abstract class PigComparisonTest
  extends FunSuite with BeforeAndAfterAll with GivenWhenThen with Logging {

  /**
   * When set, any cache files that result in test failures will be deleted.  Used when the test
   * harness or pig have been updated thus requiring new golden answers to be computed for some
   * tests. Also prevents the classpath being used when looking for golden answers as these are
   * usually stale.
   */
  val recomputeCache = System.getProperty("spark.pig.recomputeCache") != null

  protected val useSpork = java.lang.Boolean.getBoolean("spark.pig.useSpork")

  protected val shardRegEx = "(\\d+):(\\d+)".r
  /**
   * Allows multiple JVMs to be run in parallel, each responsible for portion of all test cases.
   * Format `shardId:numShards`. Shard ids should be zero indexed.  E.g. -Dspark.pig.testshard=0:4.
   */
  val shardInfo = Option(System.getProperty("spark.pig.shard")).map {
    case shardRegEx(id, total) => (id.toInt, total.toInt)
  }

  // Directory that contains our input data files
  protected val indir = System.getProperty("spark.pig.indir")
  // Directory that contains the output files from our Pig jobs
  protected val outdir = System.getProperty("spark.pig.outdir")
  // Path to the pig executable
  protected val pigCmd = System.getProperty("spark.pig.pigCmd")
  // Path to the Sigmoid Spork executable
  protected val sporkCmd = System.getProperty("spark.pig.sporkCmd")
  // Hadoop home for Pig
  protected val pigHadoop = System.getProperty("spark.pig.pigHadoop")
  // Hadoop home for Sigmoid Spork
  protected val sporkHadoop = System.getProperty("spark.pig.sporkHadoop")
  // Pig home for Pig
  protected val pigHome = System.getProperty("spark.pig.pigHome")
  // Pig home for Sigmoid Spork
  protected val sporkHome = System.getProperty("spark.pig.sporkHome")

  protected val targetDir = new File("target")

  /**
   * When set, this comma separated list is defines directories that contain the names of test cases
   * that should be skipped.
   *
   * For example when `-Dspark.pig.skiptests=passed,pigFailed` is specified and test cases listed
   * in [[passedDirectory]] or [[pigFailedDirectory]] will be skipped.
   */
  val skipDirectories =
    Option(System.getProperty("spark.pig.skiptests"))
      .toSeq
      .flatMap(_.split(","))
      .map(name => new File(targetDir, s"$suiteName.$name"))

  val runOnlyDirectories =
    Option(System.getProperty("spark.pig.runonlytests"))
      .toSeq
      .flatMap(_.split(","))
      .map(name => new File(targetDir, s"$suiteName.$name"))

  /** The local directory where cached golden answers will be stored. */
  protected val answerCache = new File("src" + File.separator + "test" +
    File.separator + "resources" + File.separator + "golden")
  if (!answerCache.exists) {
    answerCache.mkdir()
  }

  /** The local directory where cached Sigmoid Spork answers will be stored. */
  protected val sporkCache = new File("src" + File.separator + "test" +
    File.separator + "resources" + File.separator + "sigmoid")
  if (!sporkCache.exists) {
    sporkCache.mkdir()
  }

  /** The [[ClassLoader]] that contains test dependencies.  Used to look for golden answers. */
  protected val testClassLoader = this.getClass.getClassLoader

  /** Directory containing a file for each test case that passes. */
  val passedDirectory = new File(targetDir, s"$suiteName.passed")
  if (!passedDirectory.exists()) {
    passedDirectory.mkdir() // Not atomic!
  }

  /** Directory containing output of tests that fail to execute with Catalyst. */
  val failedDirectory = new File(targetDir, s"$suiteName.failed")
  if (!failedDirectory.exists()) {
    failedDirectory.mkdir() // Not atomic!
  }

  /** Directory containing output of tests where catalyst produces the wrong answer. */
  val wrongDirectory = new File(targetDir, s"$suiteName.wrong")
  if (!wrongDirectory.exists()) {
    wrongDirectory.mkdir() // Not atomic!
  }

  /** Directory containing output of tests where we fail to generate golden output with Pig. */
  val pigFailedDirectory = new File(targetDir, s"$suiteName.pigFailed")
  if (!pigFailedDirectory.exists()) {
    pigFailedDirectory.mkdir() // Not atomic!
  }

  /** All directories that contain per-query output files */
  val outputDirectories = Seq(
    passedDirectory,
    failedDirectory,
    wrongDirectory,
    pigFailedDirectory)

  protected val cacheDigest = java.security.MessageDigest.getInstance("MD5")
  protected def getMd5(str: String): String = {
    val digest = java.security.MessageDigest.getInstance("MD5")
    digest.update(str.getBytes("utf-8"))
    new java.math.BigInteger(1, digest.digest).toString(16)
  }

  protected def prepareAnswer(
                               logical: LogicalPlan,
                               answer: Seq[String]): Seq[String] = {

    def isSorted(plan: LogicalPlan): Boolean = plan match {
      case _: Join | _: Aggregate | _: BaseRelation | _: Generate | _: Sample | _: Distinct => false
      case PhysicalOperation(_, _, Sort(_, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    val orderedAnswer = answer.sorted //if (isSorted(logical)) answer else answer.sorted
    orderedAnswer.map(cleanPaths)
  }

  /**
   * Removes non-deterministic paths from `str` so cached answers will compare correctly.
   */
  protected def cleanPaths(str: String): String = {
    str.replaceAll("file:\\/.*\\/", "<PATH>")
  }

  protected class PigQueryExecution(pql: String) {
    lazy val logical = TestSQLContext.pigParser(pql)
    override def toString = pql
    def stringResult(): Seq[String] = {
      val result: Seq[Seq[Any]] = TestSQLContext.pql(pql).collect().toSeq
      // Reformat to match hive tab delimited output.
      val asString = result.map(_.mkString("\t")).toSeq
      asString
    }
  }

  protected def substituteParameters(rawQuery: String, params: Map[String, String]): String = {
    def getMatch(m: Match): String = {
      val str = m.group("param")
      params.get(str).getOrElse(throw new NoSuchElementException(str))
    }
    val paramRegex = new Regex(""":(\w+):""", "param")
    paramRegex.replaceAllIn(rawQuery, getMatch(_))
  }

  /**
   * Return the cached result for the current test, or re-run the query if we don't have the cached
   * resule
   * @param testCaseName the name of the current test
   * @param rawQuery the raw (before parameter substitution) Pig Latin query for this test
   * @param cacheFile the file that we should check for the cached result
   * @param exe path to the executable that we should call to re-run the query
   */
  protected def getCachedResult(testCaseName: String,
                                rawQuery: String,
                                cacheFile: File,
                                exe:String,
                                env:Seq[(String, String)]) = {
    val cachedFileContents = {
      logger.debug(s"Looking for cached answer file $cacheFile.")
      if (cacheFile.exists) {
        val cachedContentRdd = TestSQLContext.sparkContext.textFile(cacheFile.getAbsolutePath)
        Some(cachedContentRdd.collect())
      } else {
        logger.debug(s"File $cacheFile not found")
        None
      }
    }
    val cachedResult = cachedFileContents match {
      case None => None
      case Some(Array("")) => Some(Nil)
      case Some(Array("\n")) => Some(Seq(""))
      case Some(other) => Some(other.toSeq)
    }

    cachedResult match {
      case Some(res) =>
        logger.info(s"Using ${exe.split("/").last} cache for test: $testCaseName")
        res
      case None =>
        val params = HashMap("INPATH" -> indir, "OUTPATH" -> cacheFile.getAbsolutePath)
        val query = substituteParameters(rawQuery, params)

        //val pigQuery = new PigQueryExecution(query)
        // Make sure we can at least parse everything before attempting pig execution.
        //pigQuery.logical
        val computedResult = {
          try {
            // Kind of a hack. Technically we should be able to call
            // val stats = Pig.compile(query).bind().runSingle()
            // but that was giving weird errors.
            val cmd = s"$exe -x local -e".split(" ") :+ query
            val cmdstr = s"$exe -x local -e $query"

            logger.warn(s"Running $cmdstr")

            val pb = Process(cmdstr, None, env:_*)
            var out = List[String]()
            var err = List[String]()
            val exit = pb.!(ProcessLogger((s) => out ::= s, (s) => err ::= s))

            println(s"exit is $exit")

            if (exit != 0) {
              println("***** STDOUT *****")
              out.foreach(println)
              println("***** /STDOUT *****")
              println("***** STDERR *****")
              err.foreach(println)
              println("***** /STDERR *****")
            }

            /*
            val proc = Runtime.getRuntime.exec(cmd)
            proc.waitFor()

            println("***** STDOUT *****")
            val br = new BufferedReader(new InputStreamReader(proc.getInputStream))
            var line = br.readLine()
            while (line != null) {
              println(line)
              line = br.readLine()
            }
            println("***** /STDOUT ***")


            if (proc.exitValue() != 0) {
              println("***** STDERR *****")
              val br = new BufferedReader(new InputStreamReader(proc.getErrorStream))
              var line = br.readLine()
              while (line != null) {
                println(line)
                line = br.readLine()
              }
              println("***** /STDERR *****")
            }
            */
            val answer = TestSQLContext.sparkContext.textFile(cacheFile.getAbsolutePath)
            answer.collect()
          } catch {
            case e: Exception =>
              val errorMessage =
                s"""
                   |Failed to generate ${exe.split("/").last} answer for query:
                   |Error: ${e.getMessage}
                   |${stackTraceToString(e)}
                   |$query
                 """.stripMargin
              stringToFile(
                new File(pigFailedDirectory, testCaseName),
                errorMessage)
              fail(errorMessage)
          }
        }.toSeq
        //if (reset) { TestPig.reset() }

        computedResult
    }
  }

  val installHooksCommand = "(?i)SET.*hooks".r
  def createQueryTest(testCaseName: String, rawQuery: String, reset: Boolean = true) {
    // If test sharding is enable, skip tests that are not in the correct shard.
    shardInfo.foreach {
      case (shardId, numShards) if testCaseName.hashCode % numShards != shardId => return
      case (shardId, _) => logger.debug(s"Shard $shardId includes test '$testCaseName'")
    }

    // Skip tests found in directories specified by user.
    skipDirectories
      .map(new File(_, testCaseName))
      .filter(_.exists)
      .foreach(_ => return)

    // If runonlytests is set, skip this test unless we find a file in one of the specified
    // directories.
    val runIndicators =
      runOnlyDirectories
        .map(new File(_, testCaseName))
        .filter(_.exists)
    if (runOnlyDirectories.nonEmpty && runIndicators.isEmpty) {
      logger.debug(
        s"Skipping test '$testCaseName' not found in ${runOnlyDirectories.map(_.getCanonicalPath)}")
      return
    }

    test(testCaseName) {
      logger.debug(s"=== PIG TEST: $testCaseName ===")

      // Clear old output for this testcase.
      outputDirectories.map(new File(_, testCaseName)).filter(_.exists()).foreach(_.delete())

      lazy val rawTestCase = "\n== Raw query version of this test ==\n" + rawQuery

      try {
        val cachedAnswerName = s"$testCaseName-${getMd5(rawQuery)}"
        val pigCacheFile = new File(answerCache, cachedAnswerName)

        val pigOptions = Seq(("HADOOP_HOME", pigHadoop), ("PIG_HOME", pigHome))
        val pigResult = getCachedResult(testCaseName, rawQuery, pigCacheFile, pigCmd, pigOptions)

        val outpath = outdir + "/" + pigCacheFile.getName
        val params = HashMap("INPATH" -> indir, "OUTPATH" -> outpath)
        val query = substituteParameters(rawQuery, params)
        //val pigQuery = new PigQueryExecution(query)

        // Check that the results match unless its an EXPLAIN query.
        // We need to delete the output directory before we prepare the logical plan because the
        // Pig parser will die otherwise
        FileUtils.deleteDirectory(new File(outpath))
        val preparedPig = prepareAnswer(null, pigResult)//pigQuery.logical, pigResult)

        val testResult = {
          if (useSpork) {
            // Run w/ Spork
            val sporkCacheFile = new File(sporkCache, cachedAnswerName)
            val sporkOptions = Seq(("HADOOP_HOME", sporkHadoop), ("PIG_HOME", sporkHome))
            val sporkResult = getCachedResult(testCaseName, rawQuery, sporkCacheFile, sporkCmd, sporkOptions)
            prepareAnswer(null, sporkResult)//pigQuery.logical, sporkResult)
          }
          else {
            // Run w/ catalyst
            try {
              prepareAnswer(null, null)//pigQuery.logical, pigQuery.stringResult())
            } catch {
              case e: Throwable =>
                val errorMessage =
                  s"""
                    |Failed to execute query using catalyst:
                    |Error: ${e.getMessage}
                    |${stackTraceToString(e)}
                    |$query
                    |== PIG - ${preparedPig.size} rows ==
                    |${preparedPig.take(10)}
                    """.stripMargin
                stringToFile(new File(failedDirectory, testCaseName), errorMessage + rawTestCase)
                fail(errorMessage)
            }
          }
        }

        if (preparedPig != testResult) {
          val pigPrintOut = s"== PIG - ${preparedPig.size} rows ==" +: preparedPig.take(10)
          val engine = if (useSpork) "SPORK" else "CATALYST"
          val testPrintOut = s"== $engine - ${testResult.size} rows ==" +: testResult.take(10)

          val resultComparison = sideBySide(pigPrintOut, testPrintOut).mkString("\n")

          if (recomputeCache) {
            logger.warn(s"Clearing cache files for failed test $testCaseName")
            pigCacheFile.delete()
          }

          val errorMessage =
            s"""
              |Results do not match for $testCaseName:
              |$query\n
              |$resultComparison
              """.stripMargin

            stringToFile(new File(wrongDirectory, testCaseName), errorMessage + rawTestCase)
          fail(errorMessage)
        }

        // Touch passed file.
        new FileOutputStream(new File(passedDirectory, testCaseName)).close()
      } catch {
        case tf: org.scalatest.exceptions.TestFailedException => throw tf
        case originalException: Exception =>
          /*
          if (System.getProperty("spark.pig.canarytest") != null) {
            // When we encounter an error we check to see if the environment is still okay by running a simple query.
            // If this fails then we halt testing since something must have gone seriously wrong.
            try {
              new TestPig.PigQLQueryExecution("SELECT key FROM src").stringResult()
              TestPig.runSqlPig("SELECT key FROM src")
            } catch {
              case e: Exception =>
                logger.error(s"FATAL ERROR: Canary query threw $e This implies that the testing environment has likely been corrupted.")
                // The testing setup traps exits so wait here for a long time so the developer can see when things started
                // to go wrong.
                Thread.sleep(1000000)
            }
          }
          */
          // If the canary query didn't fail then the environment is still okay, so just throw the original exception.
          throw originalException
      }
    }
  }
}