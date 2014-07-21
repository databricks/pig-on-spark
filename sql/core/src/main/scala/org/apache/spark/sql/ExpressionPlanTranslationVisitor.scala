package org.apache.spark.sql

import org.apache.pig.newplan.logical.expression.{
LogicalExpression => PigExpression,
BinaryExpression => PigBinaryExpression,
UnaryExpression => PigUnaryExpression, _}
import org.apache.pig.newplan.DependencyOrderWalker

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, _}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.types.StringType

import scala.collection.JavaConversions._
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator

/**
 * Walks a Pig LogicalExpressionPlan tree and translates it into an equivalent Catalyst expression plan
 */
class ExpressionPlanTranslationVisitor(plan: LogicalExpressionPlan, parent: LogicalPlanTranslationVisitor)
  extends LogicalExpressionVisitor(plan, new DependencyOrderWalker(plan, true))
  with PigTranslationVisitor[PigExpression, SparkExpression] {

  // predicate ? left : right
  override def visit(pigCond: BinCondExpression) {
    val pred = getTranslation(pigCond.getCondition)
    val left = getTranslation(pigCond.getLhs)
    val right = getTranslation(pigCond.getRhs)

    val cond = new If(pred, left, right)
    updateStructures(pigCond, cond)
  }

  // TODO: Allow this to handle more general FuncSpec values
  //  (right now we can only handle casts to and from basic types)
  override def visit(pigCast: CastExpression) {
    val dstType = translateType(pigCast.getFieldSchema.`type`)
    val pigChild = pigCast.getExpression

    if (pigChild == null) {
      val alias = pigCast.getFieldSchema.alias
      val cast = new Cast(new UnresolvedAttribute(alias), dstType)
      updateStructures(pigCast, cast)
    }
    else {
      val cast = new Cast(getTranslation(pigChild), dstType)
      updateStructures(pigCast, cast)
    }
  }

  override def visit(pigConst: ConstantExpression) {
    val value = pigConst.getValue
    val sparkType = translateType(pigConst.getFieldSchema.`type`)

    val constant = new Literal(value, sparkType)
    updateStructures(pigConst, constant)
  }

  override def visit(pigLookup: MapLookupExpression) {
    val map = getTranslation(pigLookup.getMap)
    // Pig keys are always strings
    val key = new Literal(pigLookup.getLookupKey, StringType)

    val getItem = new GetItem(map, key)
    updateStructures(pigLookup, getItem)
  }

  override def visit(pigNE: NotEqualExpression) {
    val left = getTranslation(pigNE.getLhs)
    val right = getTranslation(pigNE.getRhs)

    val equals = new Equals(left, right)
    val not = new Not(equals)
    updateStructures(pigNE, not)
  }

  override def visit(pigProj: ProjectExpression) {
    if (pigProj.getColAlias != null) {
      val proj = new UnresolvedAttribute(pigProj.getColAlias)
      updateStructures(pigProj, proj)
    }
    else {
      val column = pigProj.getColNum
      val inputNum = pigProj.getInputNum
      val inputs = pigProj.getAttachedRelationalOp.getPlan.getPredecessors(pigProj.getAttachedRelationalOp)
      val sparkInput = parent.getTranslation(inputs(inputNum))
      // This works for PigLoad, but will it work for other things?
      val sparkSchema = sparkInput.output

      // HACK!HACK!HACK!
      if (sparkSchema.head.name == "") {
        val proj = BoundReference(column, AttributeReference("", StringType)())
        updateStructures(pigProj, proj)
      }
      else {
        val proj = sparkSchema(column)
        updateStructures(pigProj, proj)
      }
    }
  }

  // Unary Expressions
  override def visit(pigExp: IsNullExpression)   { unaryExpression(pigExp) }
  override def visit(pigExp: NegativeExpression) { unaryExpression(pigExp) }
  override def visit(pigExp: NotExpression)      { unaryExpression(pigExp) }

  protected def unaryExpression(pigExp: PigUnaryExpression) {
    val exp = getTranslation(pigExp.getExpression)

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
    val left = getTranslation(pigExp.getLhs)
    val right = getTranslation(pigExp.getRhs)

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
      case _: RegexExpression => RLikeExact
      case _: SubtractExpression => Subtract
    }

    val sparkExp = sparkClass(left, right)
    updateStructures(pigExp, sparkExp)
  }
}
