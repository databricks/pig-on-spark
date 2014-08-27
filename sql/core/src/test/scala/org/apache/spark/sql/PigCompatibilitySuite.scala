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