package org.apache.spark.sql

import org.apache.pig.newplan.logical.expression.{
LogicalExpression => PigExpression, BinaryExpression => PigBinaryExpression, UnaryExpression => PigUnaryExpression _}
import org.apache.pig.newplan.DependencyOrderWalker

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, _}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.types.StringType

/**
 * Walks a Pig LogicalExpressionPlan tree and translates it into an equivalent Catalyst expression plan
 */
class ExpressionPlanTranslationVisitor(plan: LogicalExpressionPlan)
  extends LogicalExpressionVisitor(plan, new DependencyOrderWalker(plan, true))
  with PigTranslationVisitor[PigExpression, SparkExpression] {

  // predicate ? left : right
  override def visit(pigCond: BinCondExpression) {
    val predicate = pigToSparkMap.get(pigCond.getCondition).getOrElse(throw new NoSuchElementException)
    val left = pigToSparkMap.get(pigCond.getLhs).getOrElse(throw new NoSuchElementException)
    val right = pigToSparkMap.get(pigCond.getRhs).getOrElse(throw new NoSuchElementException)

    val cond = new If(predicate, left, right)
    updateStructures(pigCond, cond)
  }

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

    val constant = new Literal(value, sparkType)
    updateStructures(pigConst, constant)
  }

  override def visit(pigLookup: MapLookupExpression) {
    val map = pigToSparkMap.get(pigLookup.getMap).getOrElse(throw new NoSuchElementException)
    // Pig keys are always strings
    val key = new Literal(pigLookup.getLookupKey, StringType)

    val getItem = new GetItem(map, key)
    updateStructures(pigLookup, getItem)
  }

  override def visit(pigNE: NotEqualExpression) {
    val left = pigToSparkMap.get(pigNE.getLhs).getOrElse(throw new NoSuchElementException)
    val right = pigToSparkMap.get(pigNE.getRhs).getOrElse(throw new NoSuchElementException)

    val equals = new Equals(left, right)
    val not = new Not(equals)
    updateStructures(pigNE, not)
  }

  override def visit(pigProj: ProjectExpression) {
    val column = pigProj.getColNum
    val pigSchema = pigProj.getAttachedRelationalOp.getSchema
    val sparkSchema = translateSchema(pigSchema)

    val proj = new BoundReference(column, sparkSchema(column))
    updateStructures(pigProj, proj)
  }

  // Unary Expressions
  override def visit(pigExp: IsNullExpression)   { unaryExpression(pigExp) }
  override def visit(pigExp: NegativeExpression) { unaryExpression(pigExp) }
  override def visit(pigExp: NotExpression)      { unaryExpression(pigExp) }

  protected def unaryExpression(pigExp: PigUnaryExpression) {
    val exp = pigToSparkMap.get(pigExp.getExpression).getOrElse(throw new NoSuchElementException)

    val sparkClass = pigExp match {
      case _: IsNullExpression => IsNull
      case _: NegativeExpression => UnaryMinus
      case _: NotExpression => Not
    }

    val sparkExp = sparkClass(exp)
    updateStructures(pigExp, sparkExp)
  }

  // Binary Expressions
  override def visit(pigExp: AddExpression)              { binaryExpression(pigExp) }
  override def visit(pigExp: AndExpression)              { binaryExpression(pigExp) }
  override def visit(pigExp: DivideExpression)           { binaryExpression(pigExp) }
  override def visit(pigExp: EqualExpression)            { binaryExpression(pigExp) }
  override def visit(pigExp: GreaterThanEqualExpression) { binaryExpression(pigExp) }
  override def visit(pigExp: GreaterThanExpression)      { binaryExpression(pigExp) }
  override def visit(pigExp: LessThanEqualExpression)    { binaryExpression(pigExp) }
  override def visit(pigExp: LessThanExpression)         { binaryExpression(pigExp) }
  override def visit(pigExp: ModExpression)              { binaryExpression(pigExp) }
  override def visit(pigExp: MultiplyExpression)         { binaryExpression(pigExp) }
  override def visit(pigExp: OrExpression)               { binaryExpression(pigExp) }
  override def visit(pigExp: RegexExpression)            { binaryExpression(pigExp) }
  override def visit(pigExp: SubtractExpression)         { binaryExpression(pigExp) }

  protected def binaryExpression(pigExp: PigBinaryExpression) {
    val left = pigToSparkMap.get(pigExp.getLhs).getOrElse(throw new NoSuchElementException)
    val right = pigToSparkMap.get(pigExp.getRhs).getOrElse(throw new NoSuchElementException)

    val sparkClass = pigExp match {
      case _: AddExpression => Add
      case _: AndExpression => And
      case _: DivideExpression => Divide
      case _: EqualExpression => Equals
      case _: GreaterThanEqualExpression => GreaterThanOrEqual
      case _: GreaterThanExpression => GreaterThan
      case _: LessThanEqualExpression => LessThanOrEqual
      case _: LessThanExpression => LessThan
      case _: ModExpression => Remainder
      case _: MultiplyExpression => Multiply
      case _: OrExpression => Or
      case _: RegexExpression => Like
      case _: SubtractExpression => Subtract
    }

    val sparkExp = sparkClass(left, right)
    updateStructures(pigExp, sparkExp)
  }
}
