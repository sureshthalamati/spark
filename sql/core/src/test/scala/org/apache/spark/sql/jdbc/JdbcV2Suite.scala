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

package org.apache.spark.sql.jdbc

import org.apache.spark
import org.apache.spark.sql.execution.datasources.jdbcv2.JdbcV2DataSource
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.{QueryTest, Row}
import test.org.apache.spark.sql.sources.v2.JavaSimpleDataSourceV2

/**
 * Created by suresht on 12/6/17.
 */
class JdbcV2Suite extends QueryTest with SharedSQLContext{
  import testImplicits._
  test("Jdbc implementation") {
    Seq(classOf[JdbcV2DataSource]).foreach { cls =>
      withClue(cls.getName) {
        val df = spark.read.format(cls.getName).load()
        checkAnswer(df, (0 until 10).map(i => Row(i, -i)))
        checkAnswer(df.select('j), (0 until 10).map(i => Row(-i)))
        checkAnswer(df.filter('i > 5), (6 until 10).map(i => Row(i, -i)))
      }
    }
  }
}
