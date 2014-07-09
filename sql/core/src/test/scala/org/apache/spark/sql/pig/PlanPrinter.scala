package org.apache.spark.sql

import org.apache.pig.ExecType
import org.apache.pig.PigServer
import org.apache.pig.impl.PigContext
import org.apache.pig.newplan.logical.relational.LogicalPlan
import org.apache.spark.sql.test.TestSQLContext
import TestSQLContext._

import java.util.Properties
import org.apache.spark.sql.pig.{PigLogicalPlanDumpVisitor, SparkLogicalPlanDumper}
import org.apache.commons.io.FileUtils
import java.io.File

case class DataRow(f1:Double, f2:Int, f3:Int)
case class DataRow2(f1:String, f2:Int)
case class VoterTab(name:String, age:Int, registration:String, contributions:Double)

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
    println(rdd.logicalPlan)
  }

  protected def pigInfoDump(query: String) {
    val lp = pigPlanOfQuery(query)
    val dumper = new PigLogicalPlanDumpVisitor(lp)
    dumper.visit()
  }

  protected def sparkInfoDump(query: String) {
    val rdd = sql(query)
    val dumper = new SparkLogicalPlanDumper(rdd.logicalPlan)
    dumper.dump()
  }
}
object PlanPrinter {
  def main(args: Array[String]) {
    val pp: PlanPrinter = new PlanPrinter(new PigContext(ExecType.LOCAL, new Properties))

    val inpath = "/Users/Greg/tmp/singlefile/votertab10k"
    val outpath = "/Users/Greg/tmp/out"
    FileUtils.deleteDirectory(new File(outpath))
    val pigQuery: String =
      s"""
        |a = load '$inpath' using PigStorage() as (name:chararray, age:int, registration, contributions:double);
        |b = filter a by name matches '^fred.*' and (chararray)registration matches '^dem.*';
        |store b into '$outpath' using PigStorage;
      """.stripMargin
    println("***** Pig Query *****")
    println(pigQuery + "\n")
    println("***** Pig Plan *****")
    pp.pigPrintPlan(pigQuery)
    println("\n***** Pig Dump *****")
    pp.pigInfoDump(pigQuery)

    val pigRdd = pql(pigQuery)
    println("***** Pig Result *****")
    pigRdd.collect().foreach(println)

    val data = TestSQLContext.sparkContext.textFile("../spork1.txt").map(_.split(",")).map(
      r => DataRow(r(0).toDouble, r(1).toInt, r(2).toInt))
    data.registerAsTable("spork1")
    val data2 = TestSQLContext.sparkContext.textFile("../sporkcross.txt").map(_.split(",")).map(
      r => DataRow2(r(0), r(1).toInt))
    data2.registerAsTable("sporkcross")
    val votertab10k = TestSQLContext.sparkContext.textFile(inpath).map(_.split("\t")).map(
      r => VoterTab(r(0), r(1).toInt, r(2), r(3).toDouble))
    votertab10k.registerAsTable("votertab")

    val sqlQuery: String = "SELECT * FROM votertab WHERE name LIKE 'fred%' AND registration LIKE 'dem%'"
    println("\n***** SQL Query *****")
    println(sqlQuery +  "\n")
    println("***** SQL Plan *****")
    pp.sqlPrintPlan(sqlQuery)
    println("***** SQL Dump *****")
    pp.sparkInfoDump(sqlQuery)

    val sqlRdd = sql(sqlQuery)
    println("***** SQL Result *****")
    sqlRdd.collect().foreach(println)
  }
}

