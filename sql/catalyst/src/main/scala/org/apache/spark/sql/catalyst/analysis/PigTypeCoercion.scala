/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types._

/**
 * A collection of [[catalyst.rules.Rule Rules]] that can be used to coerce differing types that
 * participate in operations into compatible ones.  Most of these rules are based on Hive semantics,
 * but they do not introduce any dependencies on the hive codebase.  For this reason they remain in
 * Catalyst until we have a more standard set of coercions.
 */
trait PigTypeCoercion extends HiveTypeCoercion {

  override val typeCoercionRules =
    PropagateTypes ::
    ConvertNaNs ::
    WidenTypes ::
    PromoteStrings ::
    PromoteByteArrays ::
    BooleanComparisons ::
    PigBooleanCasts ::
    StringToIntegralCasts ::
    FunctionArgumentConversion ::
    CastNulls ::
    Nil

  /**
   * Promotes bytearrays that appear in arithmetic expressions.
   */
  object PromoteByteArrays extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      case mod: Remainder if (mod.left.dataType == ByteArrayType
        && mod.right.dataType == ByteArrayType) =>
        sys.error("Cannot apply modulus operator to two byte arrays")
      case mod: Remainder if mod.left.dataType == ByteArrayType =>
        mod.makeCopy(Array(Cast(mod.left, mod.right.dataType), mod.right))
      case mod: Remainder if mod.left.dataType == ByteArrayType =>
        mod.makeCopy(Array(mod.left, Cast(mod.right, mod.left.dataType)))

      case a: BinaryArithmetic if a.left.dataType == ByteArrayType =>
        a.makeCopy(Array(Cast(a.left, a.right.dataType), a.right))
      case a: BinaryArithmetic if a.right.dataType == ByteArrayType =>
        a.makeCopy(Array(a.left, Cast(a.right, a.left.dataType)))

      case Average(e) if e.dataType == ByteArrayType =>
        Average(Cast(e, DoubleType))
      case Count(e) if e.dataType == ByteArrayType =>
        Count(Cast(e, LongType))
      case Max(e) if e.dataType == ByteArrayType =>
        Max(Cast(e, DoubleType))
      case Min(e) if e.dataType == ByteArrayType =>
        Min(Cast(e, DoubleType))
      case Sum(e) if e.dataType == ByteArrayType =>
        Sum(Cast(e, DoubleType))
    }
  }

  /**
   * Casts to/from [[catalyst.types.BooleanType BooleanType]] are transformed into comparisons since
   * the JVM does not consider Booleans to be numeric types.
   */
  object PigBooleanCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      // Skip nodes who's children have not been resolved yet.
      case e if !e.childrenResolved => e

      // Pig ByteArrays have different casting rules for booleans
      case Cast(e, BooleanType) if e.dataType != ByteArrayType => {
        Not(Equals(e, Literal(0)))
      }

      case Cast(e, dataType) if e.dataType == BooleanType => {
        Cast(If(e, Literal(1), Literal(0)), dataType)
      }
    }
  }
}
