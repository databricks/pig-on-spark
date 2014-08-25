package org.apache.spark.sql

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.pig.newplan.{Operator => PigOperator}
import org.apache.pig.newplan.logical.expression.{
LogicalExpression => PigExpression,
BinaryExpression => PigBinaryExpression,
UnaryExpression => PigUnaryExpression, _}
import org.apache.pig.newplan.ReverseDependencyOrderWalker

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, _}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.types.StringType

/**
 * Walks a Pig LogicalExpressionPlan tree and translates it into an equivalent Catalyst expression
 */
class ExpressionPlanTranslationVisitor(plan: LogicalExpressionPlan,
                                       parent: PigTranslationVisitor[_,_])
  extends LogicalExpressionVisitor(plan, new ReverseDependencyOrderWalker(plan))
  with PigTranslationVisitor[PigExpression, SparkExpression] {

  /**
   * Not supported: we should never use an ExpressionPlanTranslationVisitor as the parent to another
   *  ExpressionPlanTranslationVisitor
   */
  override def getSchema(pigOp: PigOperator): Bag = {
    throw new NotImplementedError("getSchema not implemented for ExpressionPlanTranslationVisitor")
  }

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

  override def visit(pigDeref: DereferenceExpression) {
    val cols = pigDeref.getBagColumns.asScala.toList

    val refExp = pigDeref.getReferredExpression
    val sparkSchema = getOutput(refExp)

    sparkSchema.projectAndUpdateStructures(cols, pigDeref, this)
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

    val equals = new EqualTo(left, right)
    val not = new Not(equals)
    updateStructures(pigNE, not)
  }

  override def visit(pigProj: ProjectExpression) {
    if (pigProj.getColAlias != null) {
      val proj = new UnresolvedAttribute(pigProj.getColAlias)
      updateStructures(pigProj, proj)
    }
    else {
      val inputNum = pigProj.getInputNum
      val relOp = pigProj.getAttachedRelationalOp
      val inputs = relOp.getPlan.getPredecessors(relOp)
      val sparkSchema = parent.getSchema(inputs(inputNum))

      val indices = {
        if (pigProj.isProjectStar) {
          (0 until sparkSchema.contents.length).toList
        }
        else if (pigProj.isRangeProject) {
          (pigProj.getStartCol to pigProj.getEndCol).toList
        }
        else {
          List(pigProj.getColNum)
        }
      }.asInstanceOf[List[Integer]]
      sparkSchema.projectAndUpdateStructures(indices, pigProj, this)
    }
  }

  protected def matchBuiltin(funcName: String, args: Seq[SparkExpression]) = {
    funcName match {
      case "SUM" => Sum(args.head)
      case "MIN" => Min(args.head)
      case "MAX" => Max(args.head)
      case "AVG" => Average(args.head)
      // We don't actually support COUNT becuase it requires projecting a bag
      //case "COUNT" => Count(args.head)
    }
  }

  override def visit(pigUDF: UserFuncExpression) {
    val args = pigUDF.getArguments.map(getTranslation)

    val funcClass = pigUDF.getFuncSpec.getClassName

    // For now, we only support Pig builtin functions
    if (!funcClass.startsWith("org.apache.pig.builtin.")) {
      throw new NotImplementedError(s"$funcClass is not a builtin function")
    }

    if (args.length > 1) {
      throw new NotImplementedError(s"UDF $funcClass takes more than 1 arg")
    }

    val funcName = funcClass.split("\\.").last

    val sparkUDF = matchBuiltin(funcName, args)
    updateStructures(pigUDF, sparkUDF)
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
      case _: EqualExpression => EqualTo
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
