package org.apache.spark.sql

import org.apache.pig.impl.PigContext
import org.apache.pig.PigServer
import org.apache.pig.newplan.logical.relational.{LogicalPlan => PigLogicalPlan}

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan => SparkLogicalPlan}

/**
 * Parses a Pig Latin query string into a Spark LogicalPlan. Hopefully.
 */
class PigParser(pc: PigContext, sc: SparkContext) {

  /**
   * Translates the given Pig Latin query into a Catalyst logical plan
   */
  def apply(input: String): SparkLogicalPlan = {
    val pigLogicalPlan = queryToPigPlan(input)
    pigPlanToSparkPlan(pigLogicalPlan)
  }

  /**
   * Converts the given Pig Latin query into a Pig LogicalPlan
   */
  def queryToPigPlan(query: String): PigLogicalPlan = {
    val pigServer: PigServer = new PigServer(pc)
    pigServer.setBatchOn
    pigServer.registerQuery(query)
    val buildLp = pigServer.getClass.getDeclaredMethod("buildLp")
    buildLp.setAccessible(true)
    buildLp.invoke(pigServer).asInstanceOf[PigLogicalPlan]
  }

  def pigPlanToSparkPlan(pigLogicalPlan: PigLogicalPlan): SparkLogicalPlan = {
    val lptv = new LogicalPlanTranslationVisitor(pigLogicalPlan, sc)
    lptv.visit()
    lptv.getRoot()
  }
}
