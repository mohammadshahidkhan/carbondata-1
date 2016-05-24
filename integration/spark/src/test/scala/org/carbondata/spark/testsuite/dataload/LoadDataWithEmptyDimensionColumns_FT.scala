/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.spark.testsuite.dataload

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * Test Class for data loading with hive syntax and old syntax
 *
 */
class LoadDataWithEmptyDimensionColumns_FT extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql(
      """
      CREATE TABLE IF NOT EXISTS emptyDimTest
      (ID Int, date Timestamp, country String,
      name String, phonetype String, serialname String, salary Int)
      STORED BY 'org.apache.carbondata.format' TBLPROPERTIES('DICTIONARY_EXCLUDE'='country')"""
    )
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    val testData = currentDirectory + "/src/test/resources/emptyDimData.csv"
    sql("LOAD DATA LOCAL INPATH '"+ testData +"' into table emptyDimTest")
  }

  test("test data loading and validate query output") {

    checkAnswer(
      sql("select name from emptyDimTest"),
      Seq(Row("aaa1"),Row("aaa2"))
    )
  }

  override def afterAll {
    sql("drop table emptyDimTest")
  }
}
