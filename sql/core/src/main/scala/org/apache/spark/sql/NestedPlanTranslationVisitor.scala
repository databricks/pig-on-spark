package org.apache.spark.sql

import scala.collection.JavaConversions._

import org.apache.pig.newplan.{Operator => PigOperator, DependencyOrderWalker,
OperatorPlan => PigOperatorPlan}
import org.apache.pig.newplan.logical.relational._

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, NamedExpression,
Alias}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.Bag._

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
  override def getSchema(pigOp: PigOperator): Bag = getOutput(pigOp)

  protected var sparkPlans: List[SparkExpression] = Nil
  protected var output: Bag = null

  def getSparkPlans = sparkPlans

  def getPlanOutput = output

  override def visit(op: LOGenerate) = {
    val plans = op.getOutputPlans
    val exprs = plans.map(translateExpression)

    // Later Pig expressions will expect this generate's output to have particular field names.
    // Minor hack: we assume that each expression produces only one field
    val aliasedOutput = exprs.zip(op.getOutputPlanSchemas).map {
      // If the output already has the desired name, no need to alias it with the same name
      case (namedExpr: NamedExpression, schema) if namedExpr.name == schema.getField(0).alias =>
        namedExpr
      case (expr, schema) =>
        Alias(expr, schema.getField(0).alias)()
    }

    sparkPlans ++= aliasedOutput
    output = bagFromSchema(aliasedOutput)
  }

  override def visit(op: LOInnerLoad) = {
    val proj = op.getProjection
    if (proj.getColAlias != null) {
      updateStructures(op, new UnresolvedAttribute(proj.getColAlias))
    }
    else {
      val column = proj.getColNum
      val inputNum = proj.getInputNum
      val forEach = op.getLOForEach
      val inputs = forEach.getPlan.getPredecessors(forEach)

      val sparkSchema = parent.getSchema(inputs(inputNum))
      sparkSchema.projectAndUpdateStructures(column, op, this)
    }
  }
}
