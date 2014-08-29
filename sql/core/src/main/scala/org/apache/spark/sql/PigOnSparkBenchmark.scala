package org.apache.spark.sql

import java.io.{File, PrintWriter}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Benchmarks a Pig query for both compilation time and execution time.
 * args(0): the text of the query to run
 * args(1): path to an output file where the results will be stored
 * args(2): the number of times to run the query (defaults to 1)
 */
object PigOnSparkBenchmark {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Pig-on-Spark Benchmark")
    val sc = new SparkContext(conf)

    // The Pig query to benchmark
    val query = args(0)
    // Output file to write results to
    val outPath = args(1)
    // Number of times to run the benchmark
    val numRuns = if (args.length > 2) args(2).toInt else 1

    val sqc = new SQLContext(sc)

    val times = (1 to numRuns).toSeq.map { i =>
      val parseT0 = System.nanoTime()
      val pigRdd = sqc.pql(query)
      val parseT1 = System.nanoTime()

      val computeT0 = System.nanoTime()
      pigRdd.collect()
      val computeT1 = System.nanoTime()

      ((parseT1 - parseT0)/1e9, (computeT1 - computeT0)/1e9)
    }

    val writer = new PrintWriter(new File(outPath))
    times.foreach { case (parseTime, computeTime) =>
      writer.write(s"$parseTime\t$computeTime\n")
    }
    writer.close()

    sc.stop()
  }
}
