package org.apache.spark.sql

import scala.collection.JavaConversions._

import org.apache.pig.newplan.{OperatorPlan => PigOperatorPlan, Operator => PigOperator, DependencyOrderWalker}
import org.apache.pig.newplan.logical.expression.{LogicalExpressionPlan => PigExpression}
import org.apache.pig.newplan.logical.relational.{LogicalPlan => PigLogicalPlan, _}

import org.apache.spark.sql.catalyst.types.{StringType, BinaryType, NullType, IntegerType}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, _}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan => SparkLogicalPlan, _}

/**
 * Walks the PigOperatorPlan and builds an equivalent SparkLogicalPlan
 */
class LogicalPlanTranslationVisitor(plan: PigOperatorPlan)
  extends LogicalRelationalNodesVisitor(plan, new DependencyOrderWalker(plan))
  with PigTranslationVisitor[PigOperator, SparkLogicalPlan] {

  override def visit(pigCross: LOCross) = {
    val inputs = pigCross.getInputs.map(getTranslation)
    var left: SparkLogicalPlan = null
    var right: SparkLogicalPlan = null

    inputs.length match {
      case x if x < 1 => throw new IllegalArgumentException("Too few inputs to CROSS")
      case 1 =>
        left = inputs.head
        right = inputs.head
      case 2 =>
        left = inputs.head
        right = inputs.tail.head
      case _ => throw new IllegalArgumentException("Too many inputs to CROSS")
    }

    // Catalyst handles an Inner join with no predicate as a Cartesian product
    // See SparkStrategies.scala:CartesianProduct
    val join = Join(left, right, Inner, None)
    updateStructures(pigCross, join)
  }

  override def visit(pigDistinct: LODistinct) = {
    val sparkChild = getChild(pigDistinct)
    val distinct = Distinct(sparkChild)
    updateStructures(pigDistinct, distinct)
  }

  override def visit(pigFilter: LOFilter) = {
    val sparkChild = getChild(pigFilter)
    val sparkExpression = translateExpression(pigFilter.getFilterPlan)
    val filter = Filter(condition = sparkExpression, child = sparkChild)
    updateStructures(pigFilter, filter)
  }

  /**
   * Pig has an annoyingly broken way of representing what type of join should be performed.
   * It uses an innerFlags array of booleans to represent which of the outputs should be treated as
   *  inner/outer sides of the join, but the interpretation of innerFlags[i] is not consistent. If
   *  all of the entries in innerFlags are true then the join is an inner join, and if all of the
   *  entries are false then the join is an outer join. This would seem to suggest that
   *  innerFlags[i] is true iff the ith input should be treated as the inner side of the join.
   *  However, a left outer join is represented by [true, false] and a right outer join is
   *  represented by [false, true], which means that in these cases innerFlags[i] means the exact
   *  opposite of what you would expect it to mean. /rant
   * This function translates Pig's bizarre design decision into a Catalyst JoinType. It was
   *  shamelessly copied from Lipstick, Netflix's open-source Pig visualizer
   *  (https://github.com/Netflix/Lipstick/blob/master/lipstick-console/src/main/java/com/netflix/
   *           lipstick/adaptors/LOJoinJsonAdaptor.java)
   */
  protected def getJoinType(node: LOJoin): JoinType = {
    val innerFlags = node.getInnerFlags()
    val sum = innerFlags.count(a => a)

    if (sum == innerFlags.length) {
      return Inner
    } else if (sum == 0) {
      return FullOuter
    } else if (innerFlags(0)) {
      return LeftOuter
    }
    return RightOuter
  }

  /**
   * Translates the Pig expressions in lefts and rights to Catalyst expresssions, then creates a
   *  list of equality expression specifying that lefts[i] = rights[i] for all i
   */
  protected def getEqualExprs(lefts: Seq[PigExpression], rights: Seq[PigExpression]) = {
    val mixed = lefts.zip(rights)
    mixed.map { case (l, r) =>
      val left = translateExpression(l)
      val right = translateExpression(r)
      Equals(left, right)
    }
  }

  override def visit(pigJoin: LOJoin) = {
    // Creates an expression that is true iff all elements of exprs are true
    def multiAnd(exprs: Seq[SparkExpression]) = { exprs.reduce(And) }

    var inputs = pigJoin.getInputs(plan.asInstanceOf[PigLogicalPlan]).map(getTranslation).toSeq
    val joinType = getJoinType(pigJoin)
    val expressionPlans = pigJoin.getExpressionPlans
    val inputNums = expressionPlans.keySet().toSeq

    val firstExprs = expressionPlans.get(inputNums.head).toSeq
    val eqSeqs = inputNums.tail.map { i =>
      val rights = expressionPlans.get(i).toSeq
      getEqualExprs(firstExprs, rights)
    }

    var ands = eqSeqs.map(multiAnd)
    /*
    //var expressions = pigJoin.getExpressionPlanValues.map(translateExpression).toSeq
    println("contents of getExpressionPlans:")
    pigJoin.getExpressionPlans.keySet().map { k: Integer =>
      val p = pigJoin.getExpressionPlans.get(k)
      println(s"k is $k, p is $p")
    }
    println("untranslated expressions: ")
    pigJoin.getExpressionPlanValues.foreach(println)
    println("translated expressions: ")
    expressions.foreach(println)
    */

    var and = ands.head
    var join = Join(inputs.head, inputs.tail.head, joinType, Some(and))

    if (inputs.length > 2) {
      if (joinType != Inner) {
        throw new IllegalArgumentException("Outer join with more than 2 inputs")
      }

      while (inputs.length >= 3) {
        and = And(ands.head, ands.tail.head)
        // Remove the second element of ands
        ands = ands.head +: ands.tail.tail
        inputs = join +: inputs.tail.tail
        join = Join(inputs.head, inputs.tail.head, Inner, Some(and))
      }
    }

    updateStructures(pigJoin, join)
  }

  override def visit(pigLimit: LOLimit) = {
    val sparkChild = getChild(pigLimit)
    val pigNum = pigLimit.getLimit
    var sparkExpression: SparkExpression = Literal(pigNum.toInt, IntegerType)

    // getLimit() returns -1 if we need to use the expression plan
    if (pigNum == -1) {
      val pigPlan = pigLimit.getLimitPlan
      if (pigPlan == null) {
        throw new IllegalArgumentException("Pig limit's number is -1 and plan is null")
      }

      sparkExpression = translateExpression(pigPlan)
    }

    val limit = Limit(sparkExpression, sparkChild)
    updateStructures(pigLimit, limit)
  }

  /**
   * This operation doesn't really have an exact analog in Spark, which uses regular Scala
   * commands to create RDDs from files.
   */
  override def visit(pigLoad: LOLoad) = {
    val file = pigLoad.getSchemaFile
    val schemaMap = translateSchema(pigLoad.getSchema)

    // This is only guaranteed to work for PigLoader, which just splits each line
    //  on a single delimiter. If no delimiter is specified, we assume tab-delimited
    val parserArgs = pigLoad.getFileSpec.getFuncSpec.getCtorArgs()
    val delimiter = if (parserArgs == null) "\t" else parserArgs(0)
    val alias = pigLoad.getAlias

    val load = PigLoad(path = file, delimiter = delimiter, alias = alias, output = schemaMap)
    updateStructures(pigLoad, load)
  }

  // TODO: Add support for UDF sort functions
  override def visit(pigSort: LOSort) = {
    val sparkChild = getChild(pigSort)
    val planDirecTups = pigSort.getSortColPlans.zip(pigSort.getAscendingCols)

    val sorts = planDirecTups.map{case (exp, dir) =>
      dir.booleanValue() match {
        case true => new SortOrder(translateExpression(exp), Ascending)
        case false => new SortOrder(translateExpression(exp), Descending)
      }
    }

    val sort = Sort(sorts.toSeq, sparkChild)
    updateStructures(pigSort, sort)
  }

  override def visit(pigStore: LOStore) = {
    val sparkChild = getChild(pigStore)
    val pathname = pigStore.getOutputSpec.getFileName
    // This is only guaranteed to work for PigLoader, which just splits each line
    //  on a single delimiter. If no delimiter is specified, we assume tab-delimited
    val parserArgs = pigStore.getFileSpec.getFuncSpec.getCtorArgs()
    val delimiter = if (parserArgs == null) "\t" else parserArgs(0)

    val store = PigStore(path = pathname, delimiter = delimiter, child = sparkChild)
    updateStructures(pigStore, store)
  }

  protected def translateExpression(pigExpression: PigExpression): SparkExpression = {
    val eptv = new ExpressionPlanTranslationVisitor(pigExpression, this)
    eptv.visit()
    eptv.getRoot()
  }

  /**
   * Parses the given schema from Pig types into Catalyst types
   */
  protected def translateSchema(pigSchema: LogicalSchema): Seq[AttributeReference] = {
    val fields = pigSchema.getFields
    val newSchema = fields.map { case field =>
      val dataType = translateType(field.`type`)
      AttributeReference(field.alias, dataType, true)()
    }
    newSchema.toSeq
  }
}
