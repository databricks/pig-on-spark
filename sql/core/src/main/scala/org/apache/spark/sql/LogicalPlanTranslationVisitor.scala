package org.apache.spark.sql

import org.apache.pig.newplan.logical.relational.{LOStore, LOFilter, LOLoad, LogicalRelationalNodesVisitor}
import org.apache.pig.newplan.DependencyOrderWalker
import org.apache.pig.newplan.{OperatorPlan => PigOperatorPlan, Operator => PigOperator}
import org.apache.pig.impl.PigContext
import org.apache.pig.newplan.logical.expression.{LogicalExpressionPlan => PigExpression}
import org.apache.pig.data.{DataType => PigDataType}

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan => SparkLogicalPlan, Filter => SparkFilter}
import org.apache.spark.sql.catalyst.expressions.{Expression => SparkExpression, _}
import org.apache.spark.sql.execution.{SparkLogicalPlan => SparkExecutionLogicalPlan}
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.HashMap
import scala.collection.immutable.List
import scala.collection.JavaConversions._
import scala.Some
import scala.Tuple2
import org.apache.spark.sql.execution.ExistingRdd
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.types.MapType
import scala.Some
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import scala.Tuple2
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation

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
   * @param loLoad
   */
  override def visit(loLoad: LOLoad) = {
    val schemaMap = schemaOfLoad(loLoad)
    val castProjection = schemaCaster(schemaMap)

    val file = loLoad.getSchemaFile
    // This is only guaranteed to work for PigLoader, which just splits each line
    //  on a single delimiter. If no delimiter is specified, we assume tab-delimited
    val parserArgs = loLoad.getFileSpec.getFuncSpec.getCtorArgs()
    val delimiter = if (parserArgs == null) "\t" else parserArgs(0)

    val splitLines = sc.sparkContext.textFile(file).map(_.split(delimiter))
    val rowRdd = splitLines.map(r => new GenericRow(r.asInstanceOf[Array[Any]]))
    val typedRdd = rowRdd.map(castProjection)
    // TODO: This is a janky hack. A cleaner public API for parsing files into schemaRDD is on our to-do list
    val schemaRDD = new SchemaRDD(
      sc, SparkExecutionLogicalPlan(ExistingRdd(schemaMap, typedRdd)))

    val alias = loLoad.getAlias
    sc.registerRDDAsTable(schemaRDD, alias)

    // We need to put something in to be the child of the next node
    // This is a hack and may not be right
    val dummyNode = new UnresolvedRelation(None, alias, None)
    updateStructures(loLoad, dummyNode)
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

    updateStructures(pigFilter, filter)
  }

  override def visit(loStore: LOStore) = {

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
   * Generates a projections that will cast an all-string row into a row
   *  with the types in schema
   */
  protected def schemaCaster(schema: Seq[AttributeReference]): MutableProjection = {
    println("schema is:")
    schema.map(println)
    val startSchema = (1 to schema.length).toSeq.map(
      i => new AttributeReference(i.toString(), StringType, nullable = true)())
    println("startSchema is:")
    startSchema.map(println)
    val casts = schema.zipWithIndex.map{case (ar, i) => Cast(startSchema(i), ar.dataType)}
    new MutableProjection(casts, startSchema)
  }

  /**
   * Returns the root of the translated SparkLogicalPlan. We're using a DependencyOrderWalker,
   *  so we're guaranteed that the last node we visit (and therefore the first node on our list)
   *  will be the root.
   * @return
   */
  def getRoot(): SparkLogicalPlan = { sparkPlans.head }

  /**
   * Sets a mapping from the (Pig) parents of the just-translated Pig operator to its translation
   *  and adds the translated operator to our list of Spark operators.
   * We translate the tree in dependency order so that we can always create a Spark node with
   *  references to its (previously translated) children. This function fills in the
   *  pigToSparkChildren map so that every Pig node has a pointer to its translated children.
   * @param pigOp
   * @param sparkOp
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

  protected def translateExpression(pigExpression: PigExpression) : SparkExpression = {
    null
  }
}
