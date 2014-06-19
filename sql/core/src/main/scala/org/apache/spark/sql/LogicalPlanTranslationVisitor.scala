package org.apache.spark.sql


import org.apache.pig.data.{DataType => PigDataType}
import org.apache.pig.impl.PigContext
import org.apache.pig.newplan.{OperatorPlan => PigOperatorPlan, Operator => PigOperator, DependencyOrderWalker}
import org.apache.pig.newplan.logical.expression.{LogicalExpressionPlan => PigExpression}
import org.apache.pig.newplan.logical.relational.{LOStore, LOFilter, LOLoad, LogicalRelationalNodesVisitor}

import org.apache.spark.sql.catalyst.expressions.{
Expression => SparkExpression, AttributeReference, GenericRow, MutableProjection, Cast}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan => SparkLogicalPlan, Filter => SparkFilter, PigLoad, PigStore}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.execution.{SparkLogicalPlan => SparkExecutionLogicalPlan, ExistingRdd}

import scala.collection.mutable.HashMap
import scala.collection.immutable.List
import scala.collection.JavaConversions._
import scala.Some
import scala.Tuple2

/**
 * Walks the PigOperatorPlan and builds an equivalent SparkLogicalPlan
 */
class LogicalPlanTranslationVisitor(plan: PigOperatorPlan, pc: PigContext, sc: SQLContext)
  extends LogicalRelationalNodesVisitor(plan, new DependencyOrderWalker(plan)) {

  // The Spark logical tree that we're building
  // Maybe this shouldn't be a list?
  protected var sparkPlans: List[SparkLogicalPlan] = Nil

  // Will map our Pig operators to the equivalent Spark operators
  // May need to make the Spark end more specific (ie. TreeNode rather than SparkLogicalPlan)
  protected val pigToSparkMap: HashMap[PigOperator, SparkLogicalPlan] = new HashMap[PigOperator, SparkLogicalPlan]

  // Maps from a Pig node to its translated children
  // This allows us to fully translate the pig node when we first visit it,
  //  since we'll already have its translated children
  protected val pigToSparkChildrenMap: HashMap[PigOperator, List[SparkLogicalPlan]] =
    new HashMap[PigOperator, List[SparkLogicalPlan]]

  /**
   * This operation doesn't really have an exact analog in Spark, which uses regular Scala
   * commands to create RDDs from files.
   * @param pigLoad
   */
  override def visit(pigLoad: LOLoad) = {
    val schemaMap = schemaOfLoad(pigLoad)
    val file = pigLoad.getSchemaFile
    // This is only guaranteed to work for PigLoader, which just splits each line
    //  on a single delimiter. If no delimiter is specified, we assume tab-delimited
    val parserArgs = pigLoad.getFileSpec.getFuncSpec.getCtorArgs()
    val delimiter = if (parserArgs == null) "\t" else parserArgs(0)
    val alias = pigLoad.getAlias

    val load = PigLoad(path = file, delimiter = delimiter, alias = alias, output = schemaMap)
    updateStructures(pigLoad, load)
  }

  override def visit(pigFilter: LOFilter) = {
    val sparkExpression = translateExpression(pigFilter.getFilterPlan)
    val sparkChild = getChild(pigFilter)
    val filter = new SparkFilter(condition = sparkExpression, child = sparkChild)
    updateStructures(pigFilter, filter)
  }

  override def visit(pigStore: LOStore) = {
    val pathname = pigStore.getOutputSpec.getFileName
    // This is only guaranteed to work for PigLoader, which just splits each line
    //  on a single delimiter. If no delimiter is specified, we assume tab-delimited
    val parserArgs = pigStore.getFileSpec.getFuncSpec.getCtorArgs()
    val delimiter = if (parserArgs == null) "\t" else parserArgs(0)

    val sparkChild = getChild(pigStore)
    val store = new PigStore(path = pathname, delimiter = delimiter, child = sparkChild)
    updateStructures(pigStore, store)
  }

  /**
   * Parses the schema of loLoad from Pig types into Catalyst types
   */
  protected def schemaOfLoad(loLoad: LOLoad): Seq[AttributeReference] = {
    val schema = loLoad.getSchema
    val fields = schema.getFields
    val newSchema = fields.map { case field =>
      val dataType = field.`type` match {
        case PigDataType.NULL => NullType
        case PigDataType.BOOLEAN => BooleanType
        case PigDataType.BYTE => ByteType
        case PigDataType.INTEGER => IntegerType
        case PigDataType.LONG => LongType
        case PigDataType.FLOAT => FloatType
        case PigDataType.DOUBLE => DoubleType
        case PigDataType.DATETIME => TimestampType
        case PigDataType.BYTEARRAY => BinaryType // Is this legit?
        case PigDataType.CHARARRAY => StringType // Is this legit?
        case PigDataType.MAP => MapType(StringType, StringType) // Hack to make the compiler happy
        case _ => {
          val typeStr = PigDataType.findTypeName(field.`type`)
          throw new Exception(s"I don't know how to handle objects of type $typeStr")
        }
      }
      AttributeReference(field.alias, dataType, true)()
    }
    newSchema.toSeq
  }

  /**
   * Returns the root of the translated SparkLogicalPlan. We're using a DependencyOrderWalker,
   *  so we're guaranteed that the last node we visit (and therefore the first node on our list)
   *  will be the root.
   */
  def getRoot(): SparkLogicalPlan = { sparkPlans.head }

  /**
   * Sets a mapping from the (Pig) parents of the just-translated Pig operator to its translation
   *  and adds the translated operator to our list of Spark operators.
   * We translate the tree in dependency order so that we can always create a Spark node with
   *  references to its (previously translated) children. This function fills in the
   *  pigToSparkChildren map so that every Pig node has a pointer to its translated children.
   */
  def updateStructures(pigOp: PigOperator, sparkOp: SparkLogicalPlan) = {
    val succs = pigOp.getPlan.getSuccessors(pigOp)
    if (succs != null) {
      for (succ <- succs) {
        val sibs = pigToSparkChildrenMap.remove(succ)

        val newSibs = sibs match {
          case None => List(sparkOp)
          case Some(realSibs) => sparkOp +: realSibs
        }

        pigToSparkChildrenMap += Tuple2(succ, newSibs)
      }
    }

    sparkPlans = sparkOp +: sparkPlans
  }

  /**
   * Returns the first child that we have stored for pigOp (which should be a UnaryNode)
   * TODO: How do we handle the case where PigOp is not a UnaryNode? Should we even check here?
   */
  protected def getChild(pigOp: PigOperator) = {
    // Get this node's children from our map and build the node
    val childList = pigToSparkChildrenMap.get(pigOp)
    childList match {
      case None => throw new NoSuchElementException
      case Some(realList) => realList.head
    }
  }

  protected def translateExpression(pigExpression: PigExpression) : SparkExpression = {
    null
  }
}
