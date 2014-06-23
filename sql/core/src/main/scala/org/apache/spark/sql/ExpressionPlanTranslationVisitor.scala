package org.apache.spark.sql

import org.apache.pig.newplan.logical.expression.{
LogicalExpression => PigExpression, BinaryExpression => PigBinaryExpression, _}
import org.apache.pig.newplan.DependencyOrderWalker

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, _}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute

/**
 * Walks a Pig LogicalExpressionPlan tree and translates it into an equivalent Catalyst expression plan
 */
class ExpressionPlanTranslationVisitor(plan: LogicalExpressionPlan)
  extends LogicalExpressionVisitor(plan, new DependencyOrderWalker(plan, true))
  with PigTranslationVisitor[PigExpression, SparkExpression] {

  // TODO: Allow this to handle more general FuncSpec values (right now we can only handle casts to and from basic types)
  override def visit(pigCast: CastExpression) {
    val alias = pigCast.getFieldSchema.alias
    val dstType = translateType(pigCast.getFieldSchema.`type`)

    val cast = new Cast(new UnresolvedAttribute(alias), dstType)
    updateStructures(pigCast, cast)
  }

  override def visit(pigConst: ConstantExpression) {
    val value = pigConst.getValue
    val sparkType = translateType(pigConst.getFieldSchema.`type`)

    val constant = Literal(value, sparkType)
    updateStructures(pigConst, constant)
  }

  override def visit(pigProj: ProjectExpression) {
    val column = pigProj.getColNum
    val pigSchema = pigProj.getAttachedRelationalOp.getSchema
    val sparkSchema = translateSchema(pigSchema)

    val proj = new BoundReference(column, sparkSchema(column))
    updateStructures(pigProj, proj)
  }

  // Binary Expressions
  override def visit(pigExp: AndExpression)              { binaryExpression(pigExp) }
  override def visit(pigExp: OrExpression)               { binaryExpression(pigExp) }
  override def visit(pigExp: EqualExpression)            { binaryExpression(pigExp) }
  override def visit(pigExp: LessThanExpression)         { binaryExpression(pigExp) }
  override def visit(pigExp: LessThanEqualExpression)    { binaryExpression(pigExp) }
  override def visit(pigExp: GreaterThanExpression)      { binaryExpression(pigExp) }
  override def visit(pigExp: GreaterThanEqualExpression) { binaryExpression(pigExp) }
  override def visit(pigExp: AddExpression)              { binaryExpression(pigExp) }
  override def visit(pigExp: SubtractExpression)         { binaryExpression(pigExp) }
  override def visit(pigExp: MultiplyExpression)         { binaryExpression(pigExp) }
  override def visit(pigExp: DivideExpression)           { binaryExpression(pigExp) }

  protected def binaryExpression(pigExp: PigBinaryExpression) {
    val left = pigToSparkMap.get(pigExp.getLhs).getOrElse(throw new NoSuchElementException)
    val right = pigToSparkMap.get(pigExp.getRhs).getOrElse(throw new NoSuchElementException)

    val sparkClass = pigExp match {
      case _: AndExpression => And
      case _: OrExpression => Or
      case _: EqualExpression => Equals
      case _: LessThanExpression => LessThan
      case _: LessThanEqualExpression => LessThanOrEqual
      case _: GreaterThanExpression => GreaterThan
      case _: GreaterThanEqualExpression => GreaterThanOrEqual
      case _: AddExpression => Add
      case _: SubtractExpression => Subtract
      case _: MultiplyExpression => Multiply
      case _: DivideExpression => Divide
    }

    val sparkExp = sparkClass(left, right)
    updateStructures(pigExp, sparkExp)
  }
}
