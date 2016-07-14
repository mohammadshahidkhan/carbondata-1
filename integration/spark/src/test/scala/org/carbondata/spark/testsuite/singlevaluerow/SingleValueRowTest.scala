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

package org.carbondata.spark.testsuite.singlevaluerow

import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.CarbonHiveContext._
import org.apache.spark.sql.common.util.QueryTest
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.scalatest.BeforeAndAfterAll

class SingleValueRowTest extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql(
      "CREATE TABLE IF NOT EXISTS singleValueRowTable (ID Int, date Timestamp, country String, " +
        "name String, " +
        "phonetype String, serialname String, salary Int) STORED BY 'org.apache.carbondata.format'"
    )
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "yyyy/mm/dd")
    val currentDirectory = new File(this.getClass.getResource("/").getPath + "/../../")
      .getCanonicalPath
    var csvFilePath = currentDirectory + "/src/test/resources/withsinglevaluerow.csv"
    sql(
      "LOAD DATA LOCAL INPATH '" + csvFilePath + "' into table " +
        "singleValueRowTable"
    );
  }

  test("select count(ID) from singleValueRowTable") {
    checkAnswer(
      sql("select count(ID) from singleValueRowTable"),
      Seq(Row(3))
    )
  }

  test("select ID from singleValueRowTable") {
    checkAnswer(
      sql("select ID from singleValueRowTable"),
      Seq(Row(1),Row(2),Row(3))
    )
  }

  override def afterAll {
    sql("drop table singleValueRowTable")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, "dd-MM-yyyy")
  }
}
