package org.apache.spark.sql

import org.apache.pig.data.{DataType => PigDataType}
import org.apache.pig.newplan.{Operator => PigOperator}

import org.apache.spark.sql.catalyst.trees.{TreeNode => SparkTreeNode}
import org.apache.spark.sql.catalyst.types._

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.pig.newplan.logical.relational.LogicalSchema

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
      case PigDataType.CHARARRAY => StringType // Is this legit?
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
   * Returns the first child that we have stored for pigOp (which should be a UnaryNode)
   * TODO: How do we handle the case where PigOp is not a UnaryNode? Should we even check here?
   */
  protected def getChild(pigOp: A) = {
    // Get this node's children from our map and build the node
    val childList = pigToSparkChildrenMap.get(pigOp)
    childList match {
      case None => throw new NoSuchElementException
      case Some(realList) => realList.head
    }
  }

  def getTranslation(pigOp: A) = {
    pigToSparkMap.get(pigOp).getOrElse(throw new NoSuchElementException)
  }

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
          case Some(realSibs) => sparkOp +: realSibs
        }

        pigToSparkChildrenMap += Tuple2(succ.asInstanceOf[A], newSibs)
      }
    }

    pigToSparkMap += Tuple2(pigOp, sparkOp)
    sparkNodes = sparkOp +: sparkNodes
  }
}

