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

package org.apache.spark.sql.execution.datasources.jdbcv2

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataSourceV2Reader, ReadTask}
import org.apache.spark.sql.types.StructType

/**
 * JDBC data source implementation using V2 data source.
 */
class JdbcV2Reader extends  DataSourceV2Reader{

  override def readSchema(): StructType = {
    new StructType().add("i", "int").add("j", "int")
  }

  override def createReadTasks(): util.List[ReadTask[Row]] = {
    val taskList = new util.ArrayList[ReadTask[Row]]()
    taskList.add(new JdbcReadTask(0, 10, readSchema()))
    taskList
  }
}
