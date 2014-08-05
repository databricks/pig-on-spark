package org.apache.spark.sql.pig

import org.apache.pig.newplan.DependencyOrderWalker
import org.apache.pig.newplan.logical.expression._
import scala.collection.JavaConversions._

class PigLogicalExpressionPlanDumpVisitor(plan: LogicalExpressionPlan, indent: String = "")
  extends LogicalExpressionVisitor(plan, new DependencyOrderWalker(plan, true)) {

  def printWithIndent(str: String) = {
    println("   " + indent + str)
  }

  def dumpExp(exp: LogicalExpression) = {
    println(indent + exp.toString)
  }

  override def visit(op: AndExpression) = { dumpExp(op) }


  override def visit(op: OrExpression) = { dumpExp(op) }


  override def visit(op: EqualExpression) = { dumpExp(op) }


  override def visit(op: ProjectExpression) = {
    dumpExp(op)
    printWithIndent("fieldSchema.alias: " + op.getFieldSchema.alias)
    printWithIndent("inputNum: " + op.getInputNum)
    printWithIndent("colNum: " + op.getColNum)
    printWithIndent("isStar: " + op.isProjectStar)
    printWithIndent("colAlias: " + op.getColAlias)
    printWithIndent("projectedOperator: " + op.getProjectedOperator)
    printWithIndent("attachedRelationalOp: " + op.getAttachedRelationalOp)
    printWithIndent("attachedRelationalOp.getSchema: " + op.getAttachedRelationalOp.getSchema)
    printWithIndent("attachedRelationalOp.predecessors:")
    op.getAttachedRelationalOp.getPlan.getPredecessors(op.getAttachedRelationalOp).map{
      pred => printWithIndent("   " + pred)
    }
  }


  override def visit(op: ConstantExpression) = { dumpExp(op) }


  override def visit(op: CastExpression) = { dumpExp(op) }


  override def visit(op: GreaterThanExpression) = { dumpExp(op) }


  override def visit(op: GreaterThanEqualExpression) = { dumpExp(op) }


  override def visit(op: LessThanExpression) = { dumpExp(op) }


  override def visit(op: LessThanEqualExpression) = { dumpExp(op) }


  override def visit(op: NotEqualExpression) = { dumpExp(op) }


  override def visit(op: NotExpression) = { dumpExp(op) }


  override def visit(op: IsNullExpression) = { dumpExp(op) }


  override def visit(op: NegativeExpression) = { dumpExp(op) }


  override def visit(op: AddExpression) = { dumpExp(op) }


  override def visit(op: SubtractExpression) = { dumpExp(op) }


  override def visit(op: MultiplyExpression) = { dumpExp(op) }


  override def visit(op: ModExpression) = { dumpExp(op) }


  override def visit(op: DivideExpression) = { dumpExp(op) }


  override def visit(op: MapLookupExpression) = { dumpExp(op) }


  override def visit(op: BinCondExpression) = { dumpExp(op) }


  override def visit(op: UserFuncExpression) = {
    dumpExp(op)
    printWithIndent("signature: " + op.getSignature)
    printWithIndent("funcSpec: " + op.getFuncSpec)
    printWithIndent("arguments: ")
    op.getArguments.foreach(a => printWithIndent("   " + a.toString))
  }


  override def visit(op: DereferenceExpression) = {
    dumpExp(op)
    printWithIndent("bagColumns: " + op.getBagColumns.mkString("[", ", ", "]"))
    printWithIndent("rawColumns: " + op.getRawColumns.mkString("[", ", ", "]"))
    printWithIndent("referredExpression: " + op.getReferredExpression)
  }


  override def visit(op: RegexExpression) = { dumpExp(op) }
}
