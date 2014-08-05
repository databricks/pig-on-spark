package org.apache.spark.sql.pig

import org.apache.pig.newplan.{Operator, DependencyOrderWalker, OperatorPlan}
import org.apache.pig.newplan.logical.relational._

import scala.collection.JavaConversions._
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan

class PigLogicalPlanDumpVisitor(plan: OperatorPlan, indent: String = "")
  extends LogicalRelationalNodesVisitor(plan, new DependencyOrderWalker(plan)) {

  def printWithIndent(str: String) = {
    println("   " + indent + str)
  }

  def dumpOp(op: Operator) = {
    println(indent + op.toString)

    printWithIndent("Predecessors:")
    val preds = op.getPlan.getPredecessors(op)
    if (preds == null) { printWithIndent("   null") }
    else preds.foreach(pred => printWithIndent("   " + pred))

    printWithIndent("Successors:")
    val succs = op.getPlan.getSuccessors(op)
    if (succs == null) { printWithIndent("   null") }
    else succs.foreach(succ => printWithIndent("   " + succ))
    println()
  }

  override def visit(op: LOLoad) = {
    dumpOp(op)
    printWithIndent("Configuration: " + op.getConfiguration)
    printWithIndent("Determined Schema: " + op.getDeterminedSchema)
    printWithIndent("Schema: " + op.getSchema)
    printWithIndent("Schema File: " + op.getSchemaFile)
    printWithIndent("Script Schema: " + op.getScriptSchema)
    printWithIndent("Signature: " + op.getSignature)
    println()
  }


  override def visit(op: LOFilter) = {
    dumpOp(op)
    printWithIndent("Filter plan:")
    val child = new PigLogicalExpressionPlanDumpVisitor(op.getFilterPlan, indent + "      ")
    child.visit()
    println()
  }

  override def visit(op: LOStore) = { dumpOp(op) }


  override def visit(op: LOJoin) = {
    dumpOp(op)
    printWithIndent("Inputs:")
    op.getInputs(plan.asInstanceOf[LogicalPlan]).map(println)
    printWithIndent("InnerFlags: " + op.getInnerFlags.mkString(","))
    printWithIndent("Join type: " + op.getJoinType)
    printWithIndent("ExpressionPlanValues:")
    op.getExpressionPlanValues.map{ expPlan =>
      val child = new PigLogicalExpressionPlanDumpVisitor(expPlan, indent + "      ")
      child.visit()
    }
    println()
  }


  override def visit(op: LOForEach) = {
    dumpOp(op)
    printWithIndent("Inner Plan:")
    val child = new PigLogicalPlanDumpVisitor(op.getInnerPlan, indent + "      ")
    child.visit()
    println()
  }


  override def visit(op: LOGenerate) = {
    dumpOp(op)
    printWithIndent("Schema: " + op.getSchema)
    printWithIndent("Output plan schemas: " + op.getOutputPlanSchemas.mkString("[", ", ", "]"))
    printWithIndent("Output Plans:")
    op.getOutputPlans.foreach { p =>
      val child = new PigLogicalExpressionPlanDumpVisitor(p, indent + "      ")
      child.visit()
      println()
    }
    printWithIndent("Plan:")
    println(op.getPlan)
    println()
  }


  override def visit(op: LOInnerLoad) = {
    dumpOp(op)
    printWithIndent("Project Expression: ")
    val child = new PigLogicalExpressionPlanDumpVisitor(
      op.getProjection.getPlan.asInstanceOf[LogicalExpressionPlan], indent + "      ")
    child.visit()
    printWithIndent("ForEach: " + op.getLOForEach)
    println()
  }


  override def visit(op: LOCube) = { dumpOp(op) }


  override def visit(op: LOCogroup) = {
    dumpOp(op)
    val plans = op.getExpressionPlans
    printWithIndent("ExpressionPlans:")
    plans.keySet().map { key =>
      val planList = plans.get(key)
      printWithIndent(s"$key: ")
      planList.map { plan =>
        val child = new PigLogicalExpressionPlanDumpVisitor(plan, indent + "      ")
        child.visit()
      }
    }
    printWithIndent("GroupType: " + op.getGroupType)
    printWithIndent("Inner: " + op.getInner.mkString("[", ", ", "]"))
    println()
  }


  override def visit(op: LOSplit) = { dumpOp(op) }


  override def visit(op: LOSplitOutput) = { dumpOp(op) }


  override def visit(op: LOUnion) = { dumpOp(op) }


  override def visit(op: LOSort) = { dumpOp(op) }


  override def visit(op: LORank) = { dumpOp(op) }


  override def visit(op: LODistinct) = { dumpOp(op) }


  override def visit(op: LOLimit) = { dumpOp(op) }


  override def visit(op: LOCross) = { dumpOp(op) }


  override def visit(op: LOStream) = { dumpOp(op) }


  override def visit(op: LONative) = { dumpOp(op) }

}
