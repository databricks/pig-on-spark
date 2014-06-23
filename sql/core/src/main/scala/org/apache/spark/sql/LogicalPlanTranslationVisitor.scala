package org.apache.spark.sql

import org.apache.pig.newplan.{OperatorPlan => PigOperatorPlan, Operator => PigOperator, DependencyOrderWalker}
import org.apache.pig.newplan.logical.expression.{LogicalExpressionPlan => PigExpression}
import org.apache.pig.newplan.logical.relational._

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan => SparkLogicalPlan, Filter => SparkFilter, PigLoad, PigStore}

import scala.collection.JavaConversions._

/**
 * Walks the PigOperatorPlan and builds an equivalent SparkLogicalPlan
 */
class LogicalPlanTranslationVisitor(plan: PigOperatorPlan)
  extends LogicalRelationalNodesVisitor(plan, new DependencyOrderWalker(plan))
  with PigTranslationVisitor[PigOperator, SparkLogicalPlan] {

  override def visit(pigFilter: LOFilter) = {
    val sparkExpression = translateExpression(pigFilter.getFilterPlan)
    val sparkChild = getChild(pigFilter)
    val filter = new SparkFilter(condition = sparkExpression, child = sparkChild)
    updateStructures(pigFilter, filter)
  }

  /**
   * This operation doesn't really have an exact analog in Spark, which uses regular Scala
   * commands to create RDDs from files.
   */
  override def visit(pigLoad: LOLoad) = {
    val schemaMap = translateSchema(pigLoad.getSchema)
    val file = pigLoad.getSchemaFile
    // This is only guaranteed to work for PigLoader, which just splits each line
    //  on a single delimiter. If no delimiter is specified, we assume tab-delimited
    val parserArgs = pigLoad.getFileSpec.getFuncSpec.getCtorArgs()
    val delimiter = if (parserArgs == null) "\t" else parserArgs(0)
    val alias = pigLoad.getAlias

    val load = PigLoad(path = file, delimiter = delimiter, alias = alias, output = schemaMap)
    updateStructures(pigLoad, load)
  }


  override def visit(pigStore: LOStore) = {
    val pathname = pigStore.getOutputSpec.getFileName
    // This is only guaranteed to work for PigLoader, which just splits each line
    //  on a single delimiter. If no delimiter is specified, we assume tab-delimited
    val parserArgs = pigStore.getFileSpec.getFuncSpec.getCtorArgs()
    val delimiter = if (parserArgs == null) "\t" else parserArgs(0)

    val sparkChild = getChild(pigStore)
    val store = new PigStore(path = pathname, delimiter = delimiter, child = sparkChild)
    updateStructures(pigStore, store)
  }

  protected def translateExpression(pigExpression: PigExpression) : SparkExpression = {
    val eptv = new ExpressionPlanTranslationVisitor(pigExpression)
    eptv.visit()
    eptv.getRoot()
  }
}
