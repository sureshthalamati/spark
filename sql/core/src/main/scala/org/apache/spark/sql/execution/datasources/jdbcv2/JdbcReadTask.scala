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

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, ReadTask}
import org.apache.spark.sql.types.StructType

/**
 * Jdbc Read task
 */
class JdbcReadTask (start: Int, end: Int, requiredSchema: StructType)
  extends ReadTask[Row] with DataReader[Row]{

  var current : Int = start -1

  override def createDataReader() : DataReader[Row] = {
    this
  }

  override def next(): Boolean = {
    current += 1
    current < end
  }

  override def get(): Row = {
    val values = requiredSchema.map(_.name).map {
      case "i" => current
      case "j" => -current
    }
    Row.fromSeq(values)
  }


  override def close(): Unit = {

  }
}
