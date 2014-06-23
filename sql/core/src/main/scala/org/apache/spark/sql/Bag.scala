package org.apache.spark.sql

import org.apache.pig.newplan.Operator

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}

/**
 * Models a Pig bag. We use these to pass complicated outputs between expressions (especially
 *  projects), when we need to represent an intermediate output that isn't legal in Catalyst
 */
case class Bag(contents: Seq[Either[NamedExpression, Bag]]) {

  /**
   * Creates a bag from the given column of this bag and tells visitor to record that bag as the
   *  output of pigOp. If the bag is a single NamedExpression, tells visitor to call
   *  updateStructures() and record that NamedExpression as the translation of pigOp.
   */
  def projectAndUpdateStructures[A <: Operator](column: Integer,
                                                pigOp: A,
                                                visitor: PigTranslationVisitor[A, Expression]): Unit = {
    val output = contents(column)
    output match {
      case Left(ne) =>
        visitor.setOutput(pigOp, Bag(Seq(output)))
        visitor.updateStructures(pigOp, ne)
      case Right(b) =>
        visitor.setOutput(pigOp, b)
        Bag.warn(pigOp, s"it projects a bag")
    }
  }

  /**
   * Creates a bag from the given columns of this bag and tells visitor to record that bag as the
   *  output of pigOp. If the bag is a single NamedExpression, tells visitor to call
   *  updateStructures() and record that NamedExpression as the translation of pigOp.
   */
  def projectAndUpdateStructures[A <: Operator](columns: List[Integer],
                                                pigOp: A,
                                                visitor: PigTranslationVisitor[A, Expression]): Unit = {
    if (columns.length == 1) {
      projectAndUpdateStructures(columns.head, pigOp, visitor)
    }
    else {
      val output = Bag(columns.map(i => contents(i)))
      visitor.setOutput(pigOp, output)
      Bag.warn(pigOp, s"it projects more than 1 column: ${columns.mkString("[", ", ", "]")}")
    }
  }
}
case object Bag {
  /**
   * Translates the schema returned from a Catalyst node.output into a Bag
   */
  def bagFromSchema(schema: Seq[NamedExpression]): Bag = Bag(schema.map(Left(_)))

  /**
   * Returns a new bag that contains each bag in bags. If there are any bags of size 1, unwraps
   *  them to be regular Attributes in the new bag. If bags contains only 1 bag, returns that bag.
   */
  def bagOfBags(bags: Seq[Bag]): Bag = {
    val elements = bags.map {
      case Bag(Seq(Left(a))) => Left(a)
      case bag => Right(bag)
    }
    elements match {
      case Seq(Right(b)) => b
      case _ => Bag(elements)
    }
  }

  /**
   * Centralized function for warnings about illegal projects.
   */
  def warn(op: Any, reason:String) = {
    println(s"Warning: not adding $op to maps because $reason")
  }
}
