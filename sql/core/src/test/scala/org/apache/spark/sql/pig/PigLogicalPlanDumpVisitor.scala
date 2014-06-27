package org.apache.spark.sql.pig

import org.apache.pig.newplan.{Operator, DependencyOrderWalker, OperatorPlan}
import org.apache.pig.newplan.logical.relational._

import scala.collection.JavaConversions._

class PigLogicalPlanDumpVisitor(plan: OperatorPlan, indent: String = "")
  extends LogicalRelationalNodesVisitor(plan, new DependencyOrderWalker(plan)) {

  def printWithIndent(str: String) = {
    println(indent + str)
  }

  def dumpOp(op: Operator) = {
    printWithIndent(op.toString)

    printWithIndent("   Predecessors:")
    val preds = op.getPlan.getPredecessors(op)
    if (preds == null) { printWithIndent("      null") }
    else {
      for (pred <- preds) {
        printWithIndent("      " + pred)
      }
    }

    printWithIndent("   Successors:")
    val succs = op.getPlan.getSuccessors(op)
    if (succs == null) { printWithIndent("      null") }
    else {
      for (succ <- succs) {
        printWithIndent("      " + succ)
      }
    }
    println()
  }

  override def visit(op: LOLoad) = { dumpOp(op) }


  override def visit(op: LOFilter) = { dumpOp(op) }


  override def visit(op: LOStore) = { dumpOp(op) }


  override def visit(op: LOJoin) = { dumpOp(op) }


  override def visit(op: LOForEach) = {
    dumpOp(op)
    printWithIndent("   Inner Plan:")
    val child = new PigLogicalPlanDumpVisitor(op.getInnerPlan, indent + "      ")
    child.visit()
    println()
  }


  override def visit(op: LOGenerate) = {
    dumpOp(op)
    printWithIndent("   Output Plans:")
    op.getOutputPlans.map({ p =>
      val child = new PigLogicalExpressionPlanDumpVisitor(p, indent + "      ")
      child.visit()
    })
    printWithIndent("   Plan:")
    println(op.getPlan)
  }


  override def visit(op: LOInnerLoad) = { dumpOp(op) }


  override def visit(op: LOCube) = { dumpOp(op) }


  override def visit(op: LOCogroup) = { dumpOp(op) }


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
