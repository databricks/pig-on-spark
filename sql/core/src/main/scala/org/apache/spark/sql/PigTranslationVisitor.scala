package org.apache.spark.sql

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import org.apache.pig.data.{DataType => PigDataType}
import org.apache.pig.newplan.{Operator => PigOperator}
import org.apache.pig.newplan.logical.expression.{LogicalExpressionPlan => PigExpressionPlan}

import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression}
import org.apache.spark.sql.catalyst.trees.{TreeNode => SparkTreeNode}
import org.apache.spark.sql.catalyst.types._

/**
 * An object that translates from the Pig node class A to the Spark node class B
 */
trait PigTranslationVisitor[A <: PigOperator, B <: SparkTreeNode[B]] {
  // The collection of translated nodes
  protected var sparkNodes: List[B] = Nil

  // Maps from a Pig node to its translation
  protected val pigToSparkMap: HashMap[A, B] = new HashMap[A, B]

  // Maps from a Pig node to its translated children
  // This allows us to fully translate the pig node when we first visit it,
  //  since we'll already have its translated children
  protected val pigToSparkChildrenMap: HashMap[A, List[B]] = new HashMap[A, List[B]]

  // Maps from a Pig node to its set of Spark outputs
  // Useful in cases where the the Spark output is not directly accessible:
  //  - For project expressions, which don't have an output field
  //  - For CoGroup logical nodes, where an intermediate output needs to be accessed before the
  //    node is fully translated
  val pigToOutputMap: HashMap[A,Bag] = new HashMap[A, Bag]

  def translateType(pigType: Byte): DataType = {
    pigType match {
      case PigDataType.NULL => NullType
      case PigDataType.BOOLEAN => BooleanType
      case PigDataType.BYTE => ByteType
      case PigDataType.INTEGER => IntegerType
      case PigDataType.LONG => LongType
      case PigDataType.FLOAT => FloatType
      case PigDataType.DOUBLE => DoubleType
      case PigDataType.DATETIME => TimestampType
      case PigDataType.BYTEARRAY => ByteArrayType
      case PigDataType.CHARARRAY => StringType
      case PigDataType.BIGINTEGER => DecimalType
      case PigDataType.BIGDECIMAL => DecimalType
      case PigDataType.MAP => MapType(StringType, StringType) // Hack to make the compiler happy
      case _ => {
        val typeStr = PigDataType.findTypeName(pigType)
        throw new Exception(s"I don't know how to handle objects of type $typeStr")
      }
    }
  }

  /**
   * Returns the root of the translated SparkLogicalPlan. We're using a DependencyOrderWalker,
   *  so we're guaranteed that the last node we visit (and therefore the first node on our list)
   *  will be the root.
   */
  def getRoot(): B = { sparkNodes.head }

  /**
   * Returns a Catalyst expression that is equivalent to the given Pig expression
   */
  protected def translateExpression(pigExpression: PigExpressionPlan): SparkExpression = {
    val eptv = new ExpressionPlanTranslationVisitor(pigExpression, this)
    eptv.visit()
    eptv.getRoot()
  }

  /**
   * Returns the first child that we have stored for pigOp (which should be a UnaryNode)
   */
  def getChild(pigOp: A): B = {
    // Get this node's children from our map and build the node
    val childList = pigToSparkChildrenMap.get(pigOp)
    childList match {
      case None =>
        val child = pigOp.getPlan.getPredecessors(pigOp).head
        throw new NoSuchElementException(child.toString)
      case Some(realList) => realList.head
    }
  }

  /**
   * Returns all children that we have stored for pigOp
   */
  def getChildren(pigOp: A): List[B] = {
    // Get this node's children from our map and build the node
    val childList = pigToSparkChildrenMap.get(pigOp)
    childList match {
      case None =>
        val children = pigOp.getPlan.getPredecessors(pigOp)
        val childrenStr = children.mkString("[", ", ", "]")
        throw new NoSuchElementException(childrenStr)
      case Some(realList) => realList
    }
  }

  /**
   * Manually registers sparkOp as the translated child of pigOp. Will override any existing
   *  mapping for pigOp. Used to cut a node out of a tree.
   */
  def setChild(pigOp: A, sparkOp: B) = {
    pigToSparkChildrenMap(pigOp) = List(sparkOp)
  }

  def getTranslation(pigOp: A) = {
    pigToSparkMap.get(pigOp).getOrElse(throw new NoSuchElementException(pigOp.toString))
  }

  /**
   * Returns the stored output for pigOp
   */
  def getOutput(pigOp: A): Bag = {
    // Get this node's children from our map and build the node
    val outputList = pigToOutputMap.get(pigOp)
    outputList match {
      case None => throw new NoSuchElementException("No output for " + pigOp)
      case Some(realBag) => realBag
    }
  }

  /**
   * Manually sets the output for pigOp. Will override any existing mapping for pigOp.
   */
  def setOutput(pigOp: A, output: Bag) = {
    pigToOutputMap(pigOp) = output
  }

  /**
   * Returns the output schema for pigOp. In most cases, we can just get the Catalyst translation
   *  of pigOp and then take its output schema. However, this doesn't work during ForEach inner
   *  plans, when we have Catalyst project expressions asking for the schema they should take as
   *  input. The project expression's input pigOp is an LOInnerLoad, but we translate LOInnerLoads
   *  to Catalyst expressions, which don't have an output schema, rather than logical nodes.
   */
  def getSchema(pigOp: PigOperator): Bag

  /**
   * Sets a mapping from the (Pig) parents of the just-translated Pig operator to its translation
   *  and adds the translated operator to our list of Spark operators.
   * We translate the tree in dependency order so that we can always create a Spark node with
   *  references to its (previously translated) children. This function fills in the
   *  pigToSparkChildren map so that every Pig node has a pointer to its translated children.
   */
  def updateStructures(pigOp: A, sparkOp: B) = {
    val succs = pigOp.getPlan.getSuccessors(pigOp)
    if (succs != null) {
      for (succ <- succs) {
        val sibs = pigToSparkChildrenMap.remove(succ.asInstanceOf[A])
        val newSibs = sibs match {
          case None => List(sparkOp)
          case Some(realSibs) => realSibs :+ sparkOp
        }

        pigToSparkChildrenMap += Tuple2(succ.asInstanceOf[A], newSibs)
      }
    }

    pigToSparkMap += Tuple2(pigOp, sparkOp)
    sparkNodes = sparkOp +: sparkNodes
  }
}

