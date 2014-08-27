package org.apache.spark.sql

import java.io._

import scala.collection.immutable.HashMap
import scala.util.matching.Regex
import scala.util.matching.Regex.Match
import scala.sys.process.{ProcessLogger, Process}

import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.hadoop.fs.Path
import org.apache.spark.util.Utils


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
  // Directory that contains the cached golden files from Pig
  protected val pigGoldenDir = System.getProperty("spark.pig.pigGoldenDir")
  // Directory that contains the cached golden files from Spork
  protected val sporkGoldenDir = System.getProperty("spark.pig.sporkGoldenDir")
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

  /** The [[ClassLoader]] that contains test dependencies.  Used to look for golden answers. */
  protected val testClassLoader = this.getClass.getClassLoader

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
      case _: Join | _: Aggregate | _: Generate | _: Sample | _: Distinct => false
      case PhysicalOperation(_, _, Sort(_, _)) => true
      case _ => plan.children.iterator.exists(isSorted)
    }

    val orderedAnswer = if (isSorted(logical)) answer else answer.sorted
    orderedAnswer.map(cleanPaths)
  }

  /**
   * Removes non-deterministic paths from `str` so cached answers will compare correctly.
   */
  protected def cleanPaths(str: String): String = {
    str.replaceAll("file:\\/.*\\/", "<PATH>")
  }

  protected class PigQueryExecution(pql: String, testCaseName: String) {
    lazy val logical = TestSQLContext.pigParser(pql)
    lazy val result: Seq[Seq[Any]] = {
      val t0 = System.nanoTime()
      val res = TestSQLContext.pql(pql).collect()
      val t1 = System.nanoTime()
      val elapsed = (t1 - t0)/1e9
      println(s"Catalyst $testCaseName\t$elapsed")
      res.toSeq
    }

    override def toString = pql
    def stringResult(): Seq[String] = {
      // Pig stores nulls as "" rather than "null"
      def cleanNulls(seq: Seq[Any]): Seq[Any] = { seq.map(e => if (e == null) "" else e ) }
      // Reformat to have consistent tab delimited output.
      val asString = result.map(cleanNulls).map(_.mkString("\t")).toSeq
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
   * @param cacheFile path to the file that we should check for the cached result
   * @param exe path to the executable that we should call to re-run the query
   */
  protected def getCachedResult(testCaseName: String,
                                rawQuery: String,
                                cacheFile: String,
                                exe:String,
                                env:Seq[(String, String)]) = {
    val cachedFileContents = {
      logger.debug(s"Looking for cached answer file $cacheFile.")
      try {
        val cachedContentRdd = TestSQLContext.sparkContext.textFile(cacheFile)
        Some(cachedContentRdd.collect())
      } catch {
        case _ : FileNotFoundException => {
          logger.debug(s"File $cacheFile not found")
          None
        }
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
        val params = HashMap("INPATH" -> indir, "OUTPATH" -> cacheFile)
        val query = substituteParameters(rawQuery, params)

        //val pigQuery = new PigQueryExecution(query)
        // Make sure we can at least parse everything before attempting pig execution.
        //pigQuery.logical
        val computedResult = {
          try {
            // Kind of a hack. Technically we should be able to call
            // val stats = Pig.compile(query).bind().runSingle()
            // but that was giving weird errors.
            val cmdstr = s"$exe -x mapreduce -e $query"

            logger.warn(s"Running $cmdstr")

            val pb = Process(cmdstr, None, env:_*)
            var out = List[String]()
            var err = List[String]()
            val t0 = System.nanoTime()
            val exit = pb.!(ProcessLogger((s) => out ::= s, (s) => err ::= s))
            val t1 = System.nanoTime()
            val elapsed = (t1 - t0)/1e9
            println(s"Reference $testCaseName\t$elapsed")

            println(s"exit is $exit")

            if (exit != 0) {
              println("***** STDOUT *****")
              out.foreach(println)
              println("***** /STDOUT *****")
              println("***** STDERR *****")
              err.foreach(println)
              println("***** /STDERR *****")
            }

            val answer = TestSQLContext.sparkContext.textFile(cacheFile)
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
              fail(errorMessage)
          }
        }.toSeq

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

    test(testCaseName) {
      logger.debug(s"=== PIG TEST: $testCaseName ===")

      // Clear old output for this testcase.
      val cachedAnswerName = s"$testCaseName-${getMd5(rawQuery)}"
      val outpath = outdir + "/" + cachedAnswerName

      val outpathHDFS = new Path(outpath)
      val fs = Utils.getHadoopFileSystem(outpath)
      fs.delete(outpathHDFS, true)

      lazy val rawTestCase = "\n== Raw query version of this test ==\n" + rawQuery

      try {

        val pigOptions = Seq(("HADOOP_HOME", pigHadoop), ("PIG_HOME", pigHome))
        val pigResult = getCachedResult(testCaseName, rawQuery, pigGoldenDir, pigCmd, pigOptions)

        val params = HashMap("INPATH" -> indir, "OUTPATH" -> outpath)
        val query = substituteParameters(rawQuery, params)
        val pigQuery = new PigQueryExecution(query, testCaseName)

        // Check that the results match unless its an EXPLAIN query.
        // We need to delete the output directory before we prepare the logical plan because the
        // Pig parser will die otherwise
        fs.delete(outpathHDFS, true)
        val preparedPig = prepareAnswer(pigQuery.logical, pigResult)

        val testResult = {
          if (useSpork) {
            // Run w/ Spork
            val sporkOptions = Seq(("HADOOP_HOME", sporkHadoop), ("PIG_HOME", sporkHome))
            val sporkResult = getCachedResult(testCaseName, rawQuery, sporkGoldenDir, sporkCmd, sporkOptions)
            prepareAnswer(pigQuery.logical, sporkResult)
          }
          else {
            // Run w/ catalyst
            try {
              prepareAnswer(pigQuery.logical, pigQuery.stringResult())
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
                fail(errorMessage)
            }
          }
        }

        if (preparedPig != testResult) {
          val pigPrintOut = s"== PIG - ${preparedPig.size} rows ==" +: preparedPig.take(10)
          val engine = if (useSpork) "SPORK" else "CATALYST"
          val testPrintOut = s"== $engine - ${testResult.size} rows ==" +: testResult.take(10)

          val resultComparison = sideBySide(pigPrintOut, testPrintOut).mkString("\n")

          val errorMessage =
            s"""
              |Results do not match for $testCaseName:
              |$query\n
              |${pigQuery.logical}\n
              |$resultComparison
              """.stripMargin

          fail(errorMessage)
        }
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