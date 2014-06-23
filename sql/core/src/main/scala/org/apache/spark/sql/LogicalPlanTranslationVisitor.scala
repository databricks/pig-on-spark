package org.apache.spark.sql

import java.util.NoSuchElementException

import scala.collection.JavaConversions._

import org.apache.pig.impl.util.MultiMap
import org.apache.pig.newplan.{OperatorPlan => PigOperatorPlan, Operator => PigOperator, DependencyOrderWalker}
import org.apache.pig.newplan.logical.expression.{LogicalExpressionPlan => PigExpressionPlan,
LogicalExpression => PigExpression}
import org.apache.pig.newplan.logical.relational.{LogicalPlan => PigLogicalPlan, _}

import org.apache.spark.sql.catalyst.types.{StringType, BinaryType, NullType, IntegerType}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, _}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan => SparkLogicalPlan, _}
import org.apache.spark.sql.Bag._

/**
 * Walks the PigOperatorPlan and builds an equivalent SparkLogicalPlan
 */
class LogicalPlanTranslationVisitor(plan: PigOperatorPlan)
  extends LogicalRelationalNodesVisitor(plan, new DependencyOrderWalker(plan))
  with PigTranslationVisitor[PigOperator, SparkLogicalPlan] {

  /**
   * The general case: first check if we have set output manually, then just get the translation
   *  of pigOp and return its output schema
   */
  override def getSchema(pigOp: PigOperator): Bag = {
    try getOutput(pigOp)
    catch {
      case _: NoSuchElementException =>
        val sparkOp = getTranslation(pigOp)
        bagFromSchema(sparkOp.output)
    }
  }

  /**
   * Returns the combined output of all sinks from the given PigExpressionPlan
   */
  def getExpPlanOutput(expPlan: PigExpressionPlan): Bag = {
    val eptv = new ExpressionPlanTranslationVisitor(expPlan, this)
    eptv.visit()
    val exprBags = expPlan.getSinks.map{ op => eptv.pigToOutputMap(op.asInstanceOf[PigExpression]) }
    bagOfBags(exprBags)
  }

  override def visit(pigCogroup: LOCogroup) = {
    val inputs = getChildren(pigCogroup)

    if (inputs.length > 1) {
      throw new NotImplementedError("We don't support cogroups on more than 1 input")
    }

    // Get grouping expressions from the expression plans
    val planMap = pigCogroup.getExpressionPlans
    // Will there always be an entry for 0? Will it always be the one we want?
    val grpExprs = planMap.get(0).map(translateExpression)

    val grpBags = planMap.get(0).map(getExpPlanOutput)
    val grpOutput = bagOfBags(grpBags)

    // TODO: We should handle the case when the output we want from an input node isn't node.output
    val output = bagOfBags(grpOutput +: inputs.map(node => bagFromSchema(node.output)))
    setOutput(pigCogroup, output)

    // Check to make sure that the cogroup is followed by a ForEach (only case we support right now)
    val next = pigCogroup.getPlan.getSuccessors(pigCogroup).head
    if (!next.isInstanceOf[LOForEach]) {
      throw new UnsupportedOperationException("CoGroup must be immediately followed by ForEach")
    }
    // Get aggregate expressions from the ForEach
    val aggExprs = getForEachExprs(next.asInstanceOf[LOForEach])

    val agg = Aggregate(grpExprs, aggExprs, inputs.head)

    // Skip over the ForEach node since we've already processed its plans
    val afterForEach = next.getPlan.getSuccessors(next)
    setOutput(next, bagFromSchema(agg.output))
    afterForEach.map(node => setChild(node, agg))

    // Kind of a hack. Now that we actually have the real output, use that
    setOutput(pigCogroup, bagFromSchema(agg.output))
    updateStructures(pigCogroup, agg)
  }

  override def visit(pigCross: LOCross) = {
    val inputs = getChildren(pigCross)

    val (left, right) = inputs.length match {
      case x if x < 1 => throw new IllegalArgumentException("Too few inputs to CROSS")
      case 1 => (inputs.head, inputs.head)
      case 2 => (inputs.head, inputs.tail.head)
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
   * If expr is already a NamedExpression, returns it. Otherwise, gives it a new alias.
   */
  protected def giveAliases(exprs: Seq[SparkExpression]): Seq[NamedExpression] = {
    def giveAlias(expr: SparkExpression, i: Int): NamedExpression = {
      expr match {
        case ne: NamedExpression => ne
        case _ => Alias(expr, s"c$i")()
      }
    }
    exprs.zipWithIndex.map{ case (e, i) => giveAlias(e, i) }
  }

  /**
   * Returns the expressions performed by this ForEach
   */
  def getForEachExprs(pigForEach: LOForEach): Seq[NamedExpression] = {
    val visitor = new NestedPlanTranslationVisitor(pigForEach.getInnerPlan, this)
    visitor.visit()
    val exprs = visitor.getSparkPlans
    // Kind of a hack. This will set the output for the ForEach even if we don't skip it
    setOutput(pigForEach, visitor.getPlanOutput)
    giveAliases(exprs)
  }

  /**
   * Returns whether or not we should skip the given ForEach node (we would want to skip it if
   *  we've already processed it as part of another node, such as a Cogroup)
   */
  def skipForEach(pigForEach: LOForEach): Boolean = {
    val succs = pigForEach.getPlan.getSuccessors(pigForEach)
    // If this ForEach was bundled up into a Cogroup, then every element of succs should have its
    //  child set to that Cogroup
    try {
      getChild(succs.head).isInstanceOf[Aggregate]
    }
    catch { case _: NoSuchElementException => false }
  }

  // TODO: Check if this works for more complicated inner plans
  override def visit(pigForEach: LOForEach) = {
    if (!skipForEach(pigForEach)) {
      val sparkChild = getChild(pigForEach)
      val exprs = getForEachExprs(pigForEach)
      val proj = Project(exprs, sparkChild)
      updateStructures(pigForEach, proj)
    }
  }

  /**
   * This function translates Pig's innerFlags array into a Catalyst JoinType. It was
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
  protected def getEqualExprs(lefts: Seq[PigExpressionPlan], rights: Seq[PigExpressionPlan]) = {
    val mixed = lefts.zip(rights)
    mixed.map { case (l, r) =>
      val left = translateExpression(l)
      val right = translateExpression(r)
      Equals(left, right)
    }
  }

  /**
   * Returns a Catalyst equijoin on the given inputs using the given Pig expressions
   * @param planMap A map from index i -> expressions used to join on inputs[i]
   * @param inputs The inputs to the join
   * @param joinType The type of join (ie. INNER, RIGHT, etc.). If inputs.length > 2, must be INNER
   */
  protected def joinFromPlanMap(planMap: MultiMap[Integer, PigExpressionPlan],
                                inputs: Seq[SparkLogicalPlan],
                                joinType: JoinType): Join = {
    // Creates an expression that is true iff all elements of exprs are true
    def multiAnd(exprs: Seq[SparkExpression]) = { exprs.reduce(And) }

    if (inputs.length > 2 && joinType != Inner) {
      throw new IllegalArgumentException("Outer join with more than 2 inputs")
    }

    val inputNums = planMap.keySet().toSeq
    val firstExprs = planMap.get(inputNums.head).toSeq
    val eqSeqs = inputNums.tail.map { i =>
      val rights = planMap.get(i).toSeq
      getEqualExprs(firstExprs, rights)
    }

    var inputsVar = inputs
    var ands = eqSeqs.map(multiAnd)
    var and = ands.head
    var join = Join(inputsVar.head, inputsVar.tail.head, joinType, Some(and))

    if (inputsVar.length > 2) {
      while (inputsVar.length >= 3) {
        and = And(ands.head, ands.tail.head)
        // Remove the second element of ands
        ands = ands.head +: ands.tail.tail
        inputsVar = join +: inputsVar.tail.tail
        join = Join(inputsVar.head, inputsVar.tail.head, joinType, Some(and))
      }
    }
    join
  }

  override def visit(pigJoin: LOJoin) = {
    val inputs = getChildren(pigJoin).toSeq
    val joinType = getJoinType(pigJoin)
    val planMap = pigJoin.getExpressionPlans

    val join = joinFromPlanMap(planMap, inputs, joinType)
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
