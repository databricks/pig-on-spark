package org.apache.spark.sql.pig

import org.apache.pig.newplan.DependencyOrderWalker
import org.apache.pig.newplan.logical.expression._

class PigLogicalExpressionPlanDumpVisitor(plan: LogicalExpressionPlan, indent: String = "")
  extends LogicalExpressionVisitor(plan, new DependencyOrderWalker(plan, true)) {

  def printWithIndent(str: String) = {
    println(indent + str)
  }

  def dumpExp(exp: LogicalExpression) = {
    printWithIndent(exp.toString)
  }

  override def visit(op: AndExpression) = { dumpExp(op) }


  override def visit(op: OrExpression) = { dumpExp(op) }


  override def visit(op: EqualExpression) = { dumpExp(op) }


  override def visit(op: ProjectExpression) = { dumpExp(op) }


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


  override def visit(op: UserFuncExpression) = { dumpExp(op) }


  override def visit(op: DereferenceExpression) = { dumpExp(op) }


  override def visit(op: RegexExpression) = { dumpExp(op) }
}
