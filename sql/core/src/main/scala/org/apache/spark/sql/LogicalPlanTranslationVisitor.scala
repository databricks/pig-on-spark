package org.apache.spark.sql

import org.apache.pig.newplan.logical.relational.{LOStore, LOFilter, LOLoad, LogicalRelationalNodesVisitor}
import org.apache.pig.newplan.DependencyOrderWalker
import org.apache.pig.newplan.{OperatorPlan => PigOperatorPlan, Operator => PigOperator}
import org.apache.pig.impl.PigContext
import org.apache.pig.newplan.logical.expression.{LogicalExpressionPlan => PigExpression}
import org.apache.pig.data.{DataType => PigDataType}

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan => SparkLogicalPlan, Filter => SparkFilter}
import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, AttributeReference, GenericRow}
import org.apache.spark.sql.execution.{SparkLogicalPlan => SparkExecutionLogicalPlan}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap
import scala.collection.immutable.List
import scala.collection.JavaConversions._
import scala.Some
import scala.Tuple2
import org.apache.spark.sql.execution.ExistingRdd

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

  override def visit(loLoad: LOLoad) = {
    val schemaMap = schemaOfLoad(loLoad)
    val file = loLoad.getSchemaFile
    val delimiter = loLoad.getFileSpec.getFuncSpec.getCtorArgs(){0}
    val rowRdd = sc.sparkContext.textFile(file).map(_.split(delimiter)).map(
      r => new GenericRow(r.asInstanceOf[Array[Any]]))
    val schemaRDD = new SchemaRDD(
      sc, SparkExecutionLogicalPlan(ExistingRdd(schemaMap, rowRdd.asInstanceOf[RDD[Row]])))
  }

  override def visit(pigFilter: LOFilter) = {
    val sparkExpression = translateExpression(pigFilter.getFilterPlan)

    // Get this node's children from our map and build the node
    val childList = pigToSparkChildrenMap.get(pigFilter)
    val sparkChild = childList match {
      case None => throw new NoSuchElementException
      case Some(realList) => realList.head
    }

    val filter: SparkFilter = new SparkFilter(condition = sparkExpression, child = sparkChild)

    // Set mappings from this object's parents to it
    val succs = pigFilter.getPlan.getSuccessors(pigFilter)
    if (succs != null) {
      for (succ <- succs) {
        val sibs = pigToSparkChildrenMap.remove(succ)

        val newSibs = sibs match {
          case None => List(filter)
          case Some(realSibs) => filter +: realSibs
        }

        pigToSparkChildrenMap += Tuple2(succ, newSibs)
      }
    }

    sparkPlans = filter +: sparkPlans
  }

  override def visit(loStore: LOStore) = {

  }

  private def schemaOfLoad(loLoad: LOLoad): List[AttributeReference] = {
    val schema = loLoad.getSchema
    var schemaMap = HashMap[String, Byte]()
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
        case PigDataType.BYTEARRAY => ArrayType // Is this legit?
        case PigDataType.CHARARRAY => StringType // Is this legit?
        case PigDataType.MAP => MapType
        case _ => {
          val typeStr = PigDataType.findTypeName(field.`type`)
          throw new Exception(s"I don't know how to handle objects of type $typeStr")
        }
      }
      AttributeReference(field.alias, dataType.asInstanceOf[DataType], true)()
    }
    newSchema.asInstanceOf[List[AttributeReference]]
  }

  /**
   * Create a SchemaRDD from a mapping from field names to types
   * TODO: We only support primitive types, add support for nested types.
   */
  /*
  private def translateSchema(schemaMap: HashMap[String, Byte]): SchemaRDD = {
    val schema = schemaMap.map { case (fieldName, fieldType) =>
      val dataType = fieldType match {
        case PigDataType.NULL => NullType
        case PigDataType.BOOLEAN => BooleanType
        case PigDataType.BYTE => ByteType
        case PigDataType.INTEGER => IntegerType
        case PigDataType.LONG => LongType
        case PigDataType.FLOAT => FloatType
        case PigDataType.DOUBLE => DoubleType
        case PigDataType.DATETIME => TimestampType
        case PigDataType.BYTEARRAY => ArrayType // Is this legit?
        case PigDataType.CHARARRAY => StringType // Is this legit?
        case PigDataType.MAP => MapType
        case _ => {
          val typeStr = PigDataType.findTypeName(fieldType)
          throw new Exception(s"I don't know how to handle objects of type $typeStr")
        }
      }
      AttributeReference(fieldName, dataType, true)()
    }.toSeq

    val rowRdd = rdd.mapPartitions { iter =>
      iter.map { map =>
        new GenericRow(map.values.toArray.asInstanceOf[Array[Any]]): Row
      }
    }
    new SchemaRDD(this, SparkLogicalPlan(ExistingRdd(schema, rowRdd)))
  }
*/

  protected def translateExpression(pigExpression: PigExpression) : SparkExpression = {
    null
  }
}
