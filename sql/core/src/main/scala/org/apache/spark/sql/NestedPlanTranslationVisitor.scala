package org.apache.spark.sql

import org.apache.pig.newplan.{Operator => PigOperator, DependencyOrderWalker, OperatorPlan => PigOperatorPlan}
import org.apache.pig.newplan.logical.relational._

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, Attribute}

import scala.collection.JavaConversions._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

/**
 * Translates the inner plan of a ForEach node. The nodes in this inner plan are technically Pig
 *  logical nodes, but map to expressions in Catalyst. Also, the most important ones (Generate and
 *  InnerLoad) only ever appear in ForEach inner plans, so it makes sense to handle them here,
 *  separately from the more general LogicalPlanTranslationVisitor
 */
class NestedPlanTranslationVisitor(plan: PigOperatorPlan, parent: LogicalPlanTranslationVisitor)
  extends LogicalRelationalNodesVisitor(plan, new DependencyOrderWalker(plan))
  with PigTranslationVisitor[PigOperator, SparkExpression] {

  /**
   * The complicated case: when a project expression appears in a ForEach nested plan, we need to
   *  get the output schema from the LOForEach node itself, rather than the LOInnerLoad node that
   *  is technically the project expression's attached operation
   */
  override def getSchema(pigOp: PigOperator): Seq[Attribute] = {
    pigOp match {
      case il: LOInnerLoad =>
        val forEach = il.getLOForEach
        val column = il.getProjection.getColNum

        val child = parent.getChild(forEach)
        Seq(child.output(column))
      case _ =>
        throw new NotImplementedError(
          "NestedPlanTranslationVisitor.getSchema only supported for LOInnerLoad")
    }
  }

  protected var projectPlans: List[SparkExpression] = Nil

  def getPlans = projectPlans

  override def visit(op: LOGenerate) = {
    val plans = op.getOutputPlans
    val exps = plans.map{ p =>
      val child = new ExpressionPlanTranslationVisitor(p, this)
      child.visit()
      child.getRoot()
    }

    projectPlans ++= exps
  }

  override def visit(op: LOInnerLoad) = {
    val pigProj = op.getProjection
    if (pigProj.getColAlias != null) {
      val proj = new UnresolvedAttribute(pigProj.getColAlias)
      updateStructures(op, proj)
    }
    else {
      val column = pigProj.getColNum
      val inputNum = pigProj.getInputNum
      val forEach = op.getLOForEach
      val inputs = forEach.getPlan.getPredecessors(forEach)

      val sparkSchema = parent.getSchema(inputs(inputNum))

      val proj = sparkSchema(column)
      updateStructures(op, proj)
    }
  }
}
