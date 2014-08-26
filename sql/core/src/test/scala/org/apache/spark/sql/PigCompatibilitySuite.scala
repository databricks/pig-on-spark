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

package org.apache.spark.sql

import java.io.File

import org.scalatest.BeforeAndAfter

/**
 * Runs the test cases that are included in the hive distribution.
 */
class PigCompatibilitySuite extends PigQueryFileTest with BeforeAndAfter {
  // TODO: bundle in jar files... get from classpath
  lazy val pigQueryDir = new File(System.getProperty("spark.pig.querydir"))

  def testCases = pigQueryDir.listFiles.map(f => f.getName -> f)

  override def beforeAll() {
  }

  override def afterAll() {
  }

  /*
  /** A list of tests deemed out of scope currently and thus completely disregarded. */
  override def blackList = Seq(
    // Pig's sort is not stable (gah!) so these give incorrect output
    // They compile and run, though, and the output seems to be properly sorted
    "Types-18",
    "Types-19",
    "Types-20",
    "Types-21",
    "Types-22",
    "Types-23",
    "Types-24",
    "Types-25",
    "Types-26",
    "Types-27",

    // These load without a schema, which we've just decided not to handle
    "Distinct-3",
    "Realias-0",
    "Unicode-0",
    "Order-3",
    "Limit-3",
    "Foreach-1",

  // These tests load from a file that is created by a store in the same query.
    // ie. a = LOAD filea; STORE a INTO fileb; b = LOAD fileb; STORE b INTO filec;
    // We don't currently support this and I don't see why you would ever want to use this when you
    // could just as easily split it up into 2 queries.
    "Bzip-0",
    "Bzip-1",
    "LoaderPigStorageArg-2",

    // Pig can't parse these because they try to cast a bytearray to a boolean
    "FilterBoolean-17",
    "FilterBoolean-18",

    // These all have a parameter substitution other than INPATH or OUTPATH, which our test harness
    // can't handle yet. Generally speaking, it's a UDF.
    "Accumulator-4",
    "Accumulator-5",
    "Accumulator-6",
    "Accumulator-7",
    "Aliases-0",
    "Bloom-0",
    "Bloom-1",
    "Bloom-2",
    "BugFix-3",
    "Casts-0",
    "Casts-1",
    "Casts-3",
    "Casts-4",
    "CastScalar-1",
    "CastScalar-4",
    "CastScalar-5",
    "CastScalar-6",
    "Checkin-1",
    "ClassResolution-0",
    "CoGroupFlatten-1",
    "CollectedGroup-0",
    "CollectedGroup-1",
    "CollectedGroup-2",
    "CollectedGroup-3",
    "CollectedGroup-4",
    "CollectedGroup-5",
    "Distinct-1",
    "EvalFunc-1",
    "EvalFunc-2",
    "EvalFunc-3",
    "EvalFunc-4",
    "FilterEq-12",
    "FilterEq-13",
    "FilterEq-5",
    "FilterEq-8",
    "FilterEq-9",
    "FilterMatches-1",
    "Foreach-11",
    "Foreach-3",
    "Foreach-4",
    "Foreach-5",
    "Foreach-6",
    "Foreach-8",
    "Foreach-9",
    "GroupAggFunc-1",
    "GroupAggFunc-3",
    "ImplicitSplit-0",
    "Join-1",
    "Join-3",
    "Join-4",
    "Limit-0",
    "Limit-1",
    "Limit-2",
    "Limit-4",
    "Limit-7",
    "Limit-8",
    "Lineage-3",
    "LoaderBinStorage-0",
    "LoaderTextLoader-0",
    "MergeJoin-0",
    "MergeJoin-1",
    "MergeJoin-2",
    "MergeJoin-3",
    "MergeJoin-4",
    "MergeJoin-5",
    "MergeJoin-6",
    "MergeJoin-7",
    "MergeSparseJoin-0",
    "MergeSparseJoin-1",
    "MergeSparseJoin-2",
    "MergeSparseJoin-3",
    "MergeSparseJoin-4",
    "MergeSparseJoin-5",
    "MissingColumns-1",
    "MonitoredUDF-0",
    "MonitoredUDF-1",
    "MonitoredUDF-2",
    "Native-0",
    "Native-1",
    "Native-2",
    "Order-1",
    "Order-10",
    "Order-11",
    "Order-12",
    "Order-13",
    "Order-14",
    "Order-15",
    "Order-16",
    "Order-17",
    "Order-5",
    "Order-6",
    "Order-7",
    "Order-8",
    "Order-9",
    "Parameters-0",
    "Parameters-1",
    "Parameters-2",
    "Parameters-3",
    "Parameters-4",
    "Partitioner-0",
    "Rank-10",
    "Rank-7",
    "Regression-0",
    "Regression-2",
    "RubyUDFs-0",
    "RubyUDFs-1",
    "RubyUDFs-10",
    "RubyUDFs-11",
    "RubyUDFs-12",
    "RubyUDFs-2",
    "RubyUDFs-3",
    "RubyUDFs-4",
    "RubyUDFs-5",
    "RubyUDFs-6",
    "RubyUDFs-7",
    "RubyUDFs-8",
    "RubyUDFs-9",
    "Scripting-0",
    "Scripting-1",
    "Scripting-10",
    "Scripting-11",
    "Scripting-2",
    "Scripting-3",
    "Scripting-4",
    "Scripting-5",
    "Scripting-6",
    "Scripting-7",
    "Scripting-8",
    "Scripting-9",
    "SecondarySort-0",
    "SecondarySort-1",
    "SecondarySort-2",
    "SecondarySort-3",
    "SecondarySort-4",
    "SecondarySort-5",
    "SecondarySort-6",
    "SecondarySort-7",
    "SecondarySort-8",
    "SkewedJoin-2",
    "SkewedJoin-5",
    "Split-0",
    "Split-1",
    "Split-2",
    "Split-3",
    "Split-7",
    "Split-8",
    "StreamingPythonUDFs-0",
    "StreamingPythonUDFs-1",
    "StreamingPythonUDFs-10",
    "StreamingPythonUDFs-11",
    "StreamingPythonUDFs-2",
    "StreamingPythonUDFs-3",
    "StreamingPythonUDFs-4",
    "StreamingPythonUDFs-5",
    "StreamingPythonUDFs-6",
    "StreamingPythonUDFs-7",
    "StreamingPythonUDFs-8",
    "StreamingPythonUDFs-9",
    "Tokenize-0",
    "Tokenize-1",
    "udf_TOBAGandTOTUPLE-0",
    "udf_TOBAGandTOTUPLE-2",
    "udf_TOBAGandTOTUPLE-3",
    "udf_TOBAGandTOTUPLE-4",
    "udf_TOBAGandTOTUPLE-5",
    "udf_TOBAGandTOTUPLE-6",
    "udf_TOBAGandTOTUPLE-7",
    "udf_TOBAGandTOTUPLE-8",
    "udf_TOBAGandTOTUPLE-9",
    "UDFContext-0",
    "UDFContextAuto-0",
    "UdfDistributedCache-0",
    "Union-2",

    // describe
    "describe-0",
    "describe-1",
    "udf_TOBAGandTOTUPLE-1",

    // rank
    "Rank-0",
    "Rank-1",
    "Rank-2",
    "Rank-3",
    "Rank-4",
    "Rank-5",
    "Rank-6",
    "Rank-8",

    // split
    "Rank-9",
    "Split-4",
    "Split-5",
    "Split-6",
    "Split-9",
    "Union-4",

    // union
    "MergeOperator-0",
    "MergeOperator-1",
    "MergeOperator-2",
    "MergeOperator-3",
    "MergeOperator-4",
    "MergeOperator-5",
    "MergeOperator-6",
    "MergeOperator-7",
    "Union-0",
    "Union-1",
    "Union-10",
    "Union-11",
    "Union-3",
    "Union-5",
    "Union-6",
    "Union-7",
    "Union-8",
    "Union-9",

    // set
    "Cross-2",

    // LOSplit and LOSplitOutput
    "CastScalar-7",

    // Seem to require data we don't have
    "Glob-7",
    "Glob-8",

    // exec
    "JsonLoaderStorage-0",
    "JsonLoaderStorage-1",
    "JsonLoaderStorage-2",
    "MergeJoin-8",

    // sample
    "Sample-0",
    "Sample-1",

    // References a column that does not exist, causing an ArrayIndexOutOfBoundsException
    "MissingColumns-0",
    "MissingColumns-2",

    // UDFs
    "SkewedJoin-6",
    "EvalFunc-0",
    "Lineage-1",
    "STRSPLIT-0",
    "Types-2",
    "Types-3",
    "Types-7",
    "Types-8",
    "ToStuffSyntaxSugar-0",
    "ToStuffSyntaxSugar-1",
    "ToStuffSyntaxSugar-2",

    // cogroup
    "Accumulator-0",
    "Accumulator-1",
    "Accumulator-2",
    "Accumulator-3",
    "Aliases-1",
    "BugFix-0",
    "BugFix-1",
    "BugFix-2",
    "CastScalar-0",
    "CastScalar-2",
    "CastScalar-3",
    "Casts-2",
    "Casts-5",
    "CoGroup-0",
    "CoGroupFlatten-0",
    "CoGroupFlatten-2",
    "CoGroupFlatten-3",
    "CoGroupFlatten-4",
    "CoGroupFlatten-5",
    "CoGroupFlatten-6",
    "Debug-3",
    "Debug-4",
    "Distinct-5",
    "FilterUdf-0",
    "FilterUdf-1",
    "Foreach-7",
    "GroupAggFunc-0",
    "GroupAggFunc-10",
    "GroupAggFunc-11",
    "GroupAggFunc-12",
    "GroupAggFunc-2",
    "ImplicitSplit-1",
    "Limit-10",
    "Limit-5",
    "Limit-6",
    "Limit-9",
    "Lineage-0",
    "NestedCross-0",
    "NestedCross-1",
    "NestedForEach-0",
    "NestedForEach-1",
    "Regression-1",
    "Scalar-0",
    "Scalar-1",
    "Scalar-2",
    "Scalar-3",
    "SecondarySort-9",
    "Types-10",
    "Types-11",
    "Types-12",
    "Types-30",
    "Types-31",
    "Types-32",
    "Types-33",
    "Types-34",
    "Types-5",
    "Types-6",
    "Types-9",

    // objects of type tuple
    "BagToString-0",
    "BagToString-1",
    "BagToTuple-0",
    "BagToTuple-1",

    // These tend to store to multiple files with similar names, and they throw a "file already exists" error
    "MapPartialAgg-0",
    "MapPartialAgg-1",
    "MapPartialAgg-2",
    "MapPartialAgg-3",
    "MapPartialAgg-4",
    "BugFix-4",
    "BugFix-5",

    // Big, scary error
    "PruneColumns-0",

    // Division by 0; looks like Pig makes this null
    "Types-1",
    "Types-35",

    // Floating point arithmetic errors :(
    "GroupAggFunc-5",
    "GroupAggFunc-7"
  )

  /**
   * The set of tests that are believed to be working in catalyst. Tests not on whiteList or
   * blacklist are implicitly marked as ignored.
   */
  override def whiteList = Seq(
    "Arithmetic-0",
    "Arithmetic-1",
    "Arithmetic-2",
    "Arithmetic-3",
    "Arithmetic-4",
    "Arithmetic-5",
    "Arithmetic-6",
    "Arithmetic-7",
    "Arithmetic-8",
    "Arithmetic-9",

    "Bincond-0",
    "Casts-6",
    "Checkin-0",
    "Cross-0",
    "Cross-1",
    "Cross-3",
    "Debug-0",
    "Debug-1",
    "Debug-2",
    "Debug-5",
    "Distinct-0",
    "Distinct-2",
    "Distinct-4",

    "FilterBoolean-0",
    "FilterBoolean-1",
    "FilterBoolean-10",
    "FilterBoolean-11",
    "FilterBoolean-12",
    "FilterBoolean-13",
    "FilterBoolean-14",
    "FilterBoolean-15",
    "FilterBoolean-16",
    "FilterBoolean-19",
    "FilterBoolean-2",
    "FilterBoolean-20",
    "FilterBoolean-21",
    "FilterBoolean-22",
    "FilterBoolean-23",
    "FilterBoolean-24",
    "FilterBoolean-25",
    "FilterBoolean-26",
    "FilterBoolean-27",
    "FilterBoolean-28",
    "FilterBoolean-3",
    "FilterBoolean-4",
    "FilterBoolean-5",
    "FilterBoolean-6",
    "FilterBoolean-7",
    "FilterBoolean-8",
    "FilterBoolean-9",
    "FilterEq-0",
    "FilterEq-1",
    "FilterEq-10",
    "FilterEq-11",
    "FilterEq-2",
    "FilterEq-3",
    "FilterEq-4",
    "FilterEq-6",
    "FilterEq-7",
    "FilterMatches-0",
    "FilterMatches-2",
    "FilterMatches-3",
    "FilterMatches-4",
    "FilterMatches-5",
    "FilterMatches-6",
    "FilterMatches-7",
    "FilterMatches-8",

    "Foreach-0",
    "Foreach-10",
    "Foreach-12",
    "Foreach-2",

    "Glob-0",
    "Glob-1",
    "Glob-2",
    "Glob-3",
    "Glob-4",
    "Glob-5",
    "Glob-6",

    "GroupAggFunc-4",
    "GroupAggFunc-6",
    "GroupAggFunc-8",
    "GroupAggFunc-9",

    "Join-0",
    "Join-10",
    "Join-11",
    "Join-12",
    "Join-2",
    "Join-5",
    "Join-6",
    "Join-7",
    "Join-8",
    "Join-9",

    "Lineage-2",
    "LoaderDefaultDir-0",
    "LoaderPigStorageArg-0",
    "LoaderPigStorageArg-1",

    "Order-0",
    "Order-18",
    "Order-2",
    "Order-4",

    "SkewedJoin-0",
    "SkewedJoin-1",
    "SkewedJoin-3",
    "SkewedJoin-4",
    "SkewedJoin-7",
    "SkewedJoin-8",
    "SkewedJoin-9",

    "ToStuffSyntaxSugar-3",

    "Types-0",
    "Types-13",
    "Types-14",
    "Types-15",
    "Types-16",
    "Types-17",
    "Types-28",
    "Types-29",
    "Types-36",
    "Types-37",
    "Types-4",
    "Types-40",
    "Types-41",
    "Types-42"
  )
  */

  override def blackList = Seq(
    "dummy"
  )

  override def whiteList = Seq(
    "L1.pig",
    "L2.pig",
    "L3.pig",
    "L4.pig",
    "L5.pig",
    "L6.pig",
    "L7.pig",
    "L8.pig",
    "L9.pig",
    "L10.pig",
    "L11.pig",
    "L12.pig",
    "L13.pig",
    "L13.pig",
    "L14.pig",
    "L15.pig",
    "L16.pig",
    "L17.pig"
  )
}