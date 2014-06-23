package org.apache.spark.sql

import org.apache.pig.ExecType
import org.apache.pig.PigServer
import org.apache.pig.impl.PigContext
import org.apache.pig.newplan.logical.relational.LogicalPlan
import org.apache.spark.sql.test.TestSQLContext
import TestSQLContext._

import java.util.Properties
import org.apache.spark.sql.pig.{PigLogicalPlanDumpVisitor, SparkLogicalPlanDumper}
import org.apache.spark.sql.execution.debug._
import org.apache.commons.io.FileUtils
import java.io.File
import scala.sys.process.Process

case class DataRow(f1:Double, f2:Int, f3:Int)
case class DataRow2(f1:String, f2:Int)
case class VoterTab(name:String, age:Int, registration:String, contributions:Double)
case class StudentTab(name:String, age:Int, gpa:Double)

class PlanPrinter(pc: PigContext) {

  protected def pigPlanOfQuery(query: String): LogicalPlan = {
    val pigServer: PigServer = new PigServer(pc)
    val newLogicalPlan: LogicalPlan = pigBuildLp(pigServer, query)
    return newLogicalPlan
  }

  protected def pigBuildLp(pigServer: PigServer, query: String): LogicalPlan = {
    pigServer.setBatchOn
    pigServer.registerQuery(query)
    val buildLp = pigServer.getClass.getDeclaredMethod("buildLp")
    buildLp.setAccessible(true)
    return buildLp.invoke(pigServer).asInstanceOf[LogicalPlan]
  }

  protected def pigPrintPlan(query: String) {
    val lp: LogicalPlan = pigPlanOfQuery(query)
    lp.explain(System.out, "text", true)
  }

  protected def sqlPrintPlan(query: String) {
    val rdd = sql(query)
    println(rdd.queryExecution.optimizedPlan)
  }

  protected def pigInfoDump(query: String) {
    val lp = pigPlanOfQuery(query)
    val dumper = new PigLogicalPlanDumpVisitor(lp)
    dumper.visit()
  }

  protected def sparkInfoDump(query: String) {
    val rdd = sql(query)
    val dumper = new SparkLogicalPlanDumper(rdd.queryExecution.optimizedPlan)
    dumper.dump()
  }
}
object PlanPrinter {
  def main(args: Array[String]) {
    val pp: PlanPrinter = new PlanPrinter(new PigContext(ExecType.LOCAL, new Properties))

    val inpath = "/Users/Greg/tmp/singlefile"
    val outpath = "/Users/Greg/tmp/out"
    FileUtils.deleteDirectory(new File(outpath))
    val pigQuery: String =
      s"""
        |a = load '$inpath/votertab10k' using PigStorage() as (name, age:int, registration, contributions);
        |e = group a by name  parallel 8;
        |f = foreach e generate group, MAX(a.contributions), MIN(a.contributions) ;
        |store f into '$outpath';
      """.stripMargin

    println("***** Pig Query *****")
    println(pigQuery + "\n")

    println("\n***** Pig Dump *****")
    pp.pigInfoDump(pigQuery)
    println("***** Pig Plan *****")
    pp.pigPrintPlan(pigQuery)

    println("About to parse")
    val parsed = TestSQLContext.parsePig(pigQuery)
    println("***** Translated Plan *****")
    println(parsed)

    println("***** Translated Plan Dump *****")
    val dumper = new SparkLogicalPlanDumper(parsed)
    dumper.dump()
/*

    println("***** Pig Result *****")
    val pigRdd = pql(pigQuery)
    println(pigRdd.queryExecution)
    FileUtils.deleteDirectory(new File(outpath))
    pigRdd.debug(TestSQLContext.sparkContext)

    //val sparkOut = pigRdd.collect().sortBy(row => row(0).asInstanceOf[String])

    FileUtils.deleteDirectory(new File(outpath))
    val exe = "/Users/Greg/Pig/pig/bin/pig"
    val cmdstr = s"$exe -x local -e $pigQuery"
    val pb = Process(cmdstr, None)
    pb.!
    */
    /*
    val answer = TestSQLContext.sparkContext.textFile("/Users/Greg/Spark/spark/sql/core/src/test/resources/golden/Accumulator-0-22288e822ed4ddadd63566139b358135")
    val pigOut = answer.collect().map{ str =>
      val arr = str.split("\t")
      (arr.take(arr.length - 1).mkString(" "), arr.last.toLong)
    }.sortBy(_._1)

    sparkOut.zip(pigOut).map{ case (row, tup) =>
      (row(0), row(1).asInstanceOf[Long].toDouble/tup._2)
    }.foreach(println)
*/
    /*
    val output = pigRdd.sortBy(row => row(1).asInstanceOf[Long]).collect()
    output.zip(output(0) +: output.take(output.length - 1)).map { case (row1, row2) =>
      (row1(0), row1(1).asInstanceOf[Long] - row2(1).asInstanceOf[Long])
    }.sortBy(_._1.asInstanceOf[String]).foreach(println)
*/

    val data = TestSQLContext.sparkContext.textFile("../spork1.txt").map(_.split(",")).map(
      r => DataRow(r(0).toDouble, r(1).toInt, r(2).toInt))
    data.registerAsTable("spork1")
    val data2 = TestSQLContext.sparkContext.textFile("../sporkcross.txt").map(_.split(",")).map(
      r => DataRow2(r(0), r(1).toInt))
    data2.registerAsTable("sporkcross")
    val studenttab10k = TestSQLContext.sparkContext.textFile(inpath + "/studenttab10").map(_.split("\t")).map(
      r => StudentTab(r(0), r(1).toInt, r(2).toDouble))
    studenttab10k.registerAsTable("studenttab")
    val votertab10k = TestSQLContext.sparkContext.textFile(inpath + "/votertab10").map(_.split("\t")).map(
      r => VoterTab(r(0), r(1).toInt, r(2), r(3).toDouble))
    votertab10k.registerAsTable("votertab")

    val sqlQuery: String =
      """
        |SELECT name, COUNT(*), MAX(contributions), MIN(contributions)
        |FROM votertab
        |GROUP BY name
      """.stripMargin

    println("\n***** SQL Query *****")
    println(sqlQuery +  "\n")

    println("***** SQL Plan *****")
    pp.sqlPrintPlan(sqlQuery)
/*
    println("***** SQL Dump *****")
    pp.sparkInfoDump(sqlQuery)

    val sqlRdd = sql(sqlQuery)
    println("***** SQL Result *****")
    sqlRdd.collect().foreach(println)
*/
  }
}
