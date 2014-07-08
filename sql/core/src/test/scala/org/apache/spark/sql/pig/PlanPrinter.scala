package org.apache.spark.sql

import org.apache.pig.ExecType
import org.apache.pig.PigServer
import org.apache.pig.impl.PigContext
import org.apache.pig.newplan.logical.relational.LogicalPlan
import org.apache.spark.sql.test.TestSQLContext
import TestSQLContext._

import java.util.Properties
import org.apache.spark.sql.pig.{PigLogicalPlanDumpVisitor, SparkLogicalPlanDumper}

case class DataRow(f1:Double, f2:Int, f3:Int)
case class DataRow2(f1:String, f2:Int)

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
    val pigQuery: String = (
      "a = LOAD '../spork1.txt' USING PigStorage(',') AS (f1:double, f2:int, f3:int);"
        + "b = LOAD '../sporkcross.txt' USING PigStorage(',') AS (f1:chararray, f2:int);"
        + "c = JOIN a BY f1 LEFT, b BY f2;"
        + "STORE c INTO '../spork2.txt' USING PigStorage(',');")
    println("***** Pig Query *****")
    println(pigQuery + "\n")
    println("***** Pig Plan *****")
    pp.pigPrintPlan(pigQuery)
    println("\n***** Pig Dump *****")
    pp.pigInfoDump(pigQuery)

    val data = TestSQLContext.sparkContext.textFile("../spork1.txt").map(_.split(",")).map(
      r => DataRow(r(0).toDouble, r(1).toInt, r(2).toInt))
    data.registerAsTable("spork1")
    val data2 = TestSQLContext.sparkContext.textFile("../sporkcross.txt").map(_.split(",")).map(
      r => DataRow2(r(0), r(1).toInt))
    data2.registerAsTable("sporkcross")
    val sqlQuery: String = "SELECT f1, f2, f3 FROM spork1 LEFT JOIN sporkcross ON spork1.f1=sporkcross.f2"
    println("\n***** SQL Query *****")
    println(sqlQuery +  "\n")
    println("***** SQL Plan *****")
    pp.sqlPrintPlan(sqlQuery)
    println("***** SQL Dump *****")
    pp.sparkInfoDump(sqlQuery)
  }
}

