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

import java.math.BigDecimal
import java.sql.{Date, DriverManager, Timestamp}
import java.util.{Calendar, GregorianCalendar, Properties}

import org.apache.spark.sql.execution.DataSourceScanExec
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRDD, JdbcUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, DataFrame, Row}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkException, SparkFunSuite}
import org.h2.jdbc.JdbcSQLException
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

class JDBCFunctionPushDownSuite extends QueryTest
  with BeforeAndAfter with PrivateMethodTester with SharedSQLContext {
  import testImplicits._

  val url = "jdbc:h2:mem:testdb0"
  val urlWithUserAndPass = "jdbc:h2:mem:testdb0;user=testUser;password=testPass"
  var conn: java.sql.Connection = null

  val testBytes = Array[Byte](99.toByte, 134.toByte, 135.toByte, 200.toByte, 205.toByte)

  val testH2Dialect = new JdbcDialect {
    override def canHandle(url: String) : Boolean = url.startsWith("jdbc:h2")
    override def getCatalystType(
        sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] =
      Some(StringType)
  }

  before {
    Utils.classForName("org.h2.Driver")
    // Extra properties that will be specified for our database. We need these to test
    // usage of parameters from OPTIONS clause in queries.
    val properties = new Properties()
    properties.setProperty("user", "testUser")
    properties.setProperty("password", "testPass")
    properties.setProperty("rowId", "false")

    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement(
      "create table test.orders(id INTEGER NOT NULL, itemname TEXT(32) NOT NULL, " +
        "itemcount integer, orderdate date)").executeUpdate()
    conn.prepareStatement(
      "insert into test.orders values " +
        "(1, 'iphone6', 9, DATE '2015-12-31')").executeUpdate()
    conn.prepareStatement(
      "insert into test.orders values " +
        "(2, 'nexus6p', 5, DATE '2016-01-31')").executeUpdate()

    conn.prepareStatement(
      "insert into test.orders values " +
        "(3, 'galaxy s7', 7, DATE '2016-05-31')").executeUpdate()
    conn.commit()

    sql(
      s"""
        |CREATE TEMPORARY TABLE orders
        |USING org.apache.spark.sql.jdbc
        |OPTIONS (url '$url', dbtable 'TEST.ORDERS', user 'testUser', password 'testPass')
      """.stripMargin.replaceAll("\n", " "))
  }

  after {
    conn.close()
  }

  test("simple select") {
    val df = sql("SELECT * FROM orders where itemcount = 5")
    df.explain(true)
    df.show()
  }

  test("Function used only used in the predicate.") {
    val df = sql("SELECT * FROM orders where year(orderdate) = 2016")
    df.explain(true)
    df.show()
  }

  test("Function used as part of expression in  predicate.") {
    val df = sql("SELECT * FROM orders where year(ordertime) +1 = 2016")
    df.explain(true)
    df.show()
  }

  test("Function in derived table  select list and the predicate on function result.") {
    val df = sql("SELECT * FROM (select itemname, year(orderdate)" +
      " as orderyear from orders) where orderyear = 2016")
    df.explain(true)
    df.show()
  }

  test("Partial push down of predicate.") {
    spark.udf.register("myStrlen", (_: String).length)
    val df = sql("SELECT * FROM orders where myStrlen(itemname) = 7 and itemcount > 5")
    df.explain(true)
    df.show()
  }

  test("Function used in the select list and the predicate.") {
    val df = sql(
      "SELECT itemname, year(orderdate) as orderyear FROM orders where year(orderdate) = 2016")
    df.explain(true)
    df.show()
  }

  test("Spark udf function, should not be pushed down even if name is same.") {
    spark.udf.register("abs", (_: String).length)
    val df = sql("SELECT * FROM orders where abs(itemname) = 7")
    df.explain(true)
    df.show()
  }

  test("Function using SQL syntax.") {
   sql("CREATE TEMPORARY FUNCTION myconcat AS 'java.lang.String'")
    val df = sql("SELECT myconcat('A:', itemname) FROM orders")
    df.explain(true)
    df.show()
  }

  test("SELECT function") {
    assert(sql("SELECT * FROM foobar where abs(theid) < 1100").collect().size === 3)
  }

  test("SELECT expression ") {
    // val df = sql("SELECT abs(theid) FROM foobar where 2+ theid = 1")
    val df = sql("SELECT * FROM foobar where abs(theid) = abs(-1)")
    df.explain(true)
    df.show()
    // assert(df.collect().size === 3)
  }

  test("SELECT subquery expression ") {
    // val df = sql("SELECT abs(theid) FROM foobar where 2+ theid = 1")
    // DOES NOT COMPILE NOT SURE , WHY :
    // val df = sql("select * from foobar where EXITS (select theid+1 from foobar where theid > 1)")
    val df = sql("select * from orders where id in  (select id+1 from orders where id > 1)")
    df.explain(true)
    df.show()
    // assert(df.collect().size === 3)
  }

  test("agrregate") {
    val df = sql("SELECT sum(theid) FROM foobar where theid = 1")
    df.explain(true)
    assert(df.collect().size === 1)
  }

  test("count agrregate") {
    val df = sql("SELECT count(id)  FROM orders")
    df.explain(true)
    df.show
    // assert(df.collect().size === 3)
  }

  /*
     conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema test").executeUpdate()
    conn.prepareStatement(
      "create table test.orders(id INTEGER NOT NULL, itemname TEXT(32) NOT NULL, " +
        "itemcount integer, ordertime timestamp)").executeUpdate()
    conn.prepareStatement(
      "insert into test.orders values " +
        "(1, 'iphone6', 9, TIMESTAMP '2015-12-31 12:59:59')").executeUpdate()
    conn.prepareStatement(
      "insert into test.orders values " +
        "(2, 'nexus6p', 5, TIMESTAMP '2016-01-31 18:01:29')").executeUpdate()

    conn.prepareStatement(
      "insert into test.orders values " +
        "(3, 'galaxy s7', 7, TIMESTAMP '2016-05-31 06:18:19')").executeUpdate()
    conn.commit()
   */


  // test math functions

  // test string functions

  test("string concat") {
    val df = Seq[(String, String, String)](("a", "b", null)).toDF("a", "b", "c")
    // CAST support is trickey. Because of conversion from spark type to data source type mapping.
    // val cdf =
    // sql("SELECT id, itemname FROM orders where concat(id, '-', itemname) = '1-iphone6'")

    val cdf = sql("SELECT id , itemname FROM orders where concat('-', itemname) = '-iphone6'")

    cdf.explain(true)
    checkAnswer(cdf, Row(1, "iphone6"))
  }

  test("string concat_ws") {
    val cdf =
      sql("SELECT id , itemname FROM orders" +
        " where concat_ws('-', '2016', itemname) = '2016-iphone6'")
    cdf.explain(true)
    checkAnswer(cdf, Row(1, "iphone6"))
  }

  // FAILES elt not supported on H2
  test("string elt") {
    val cdf =
      sql("SELECT id , itemname FROM orders" +
        " where elt(2, '2016', itemname) = 'iphone6'")
    cdf.explain(true)
    checkAnswer(cdf, Row(1, "iphone6"))
  }

  test("Substring") {
    val cdf =
      sql("SELECT id , itemname FROM orders" +
        " where Substring(itemname, 0, 6) = 'iphone'")

    cdf.explain(true)
    checkAnswer(cdf, Row(1, "iphone6"))
  }

  test("string substring_index function") {
    val cdf =
      sql("SELECT id , itemname FROM orders" +
        " where substring_index(itemname, 'o', 1) = 'iph'")

    cdf.explain(true)
    checkAnswer(cdf, Row(1, "iphone6"))
  }

  test("string substring_index1 function") {
    val cdf =
      sql("SELECT id , itemname FROM orders" +
        " where substring_index(itemname, 'o', 1) = 'iph'")

    cdf.explain(true)
    checkAnswer(cdf, Row(1, "iphone6"))
  }

  test("addition operator") {
    val cdf =
      sql("SELECT id , itemname FROM orders where itemcount - 3  = 6")

    cdf.explain(true)
    checkAnswer(cdf, Row(1, "iphone6"))
  }

  /*
  test("simple binary tree creation from static input") {
    val input = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9)
    // sort the list.
    // find the middle , and keep spliting. and create node objects.
    case class Node (var root: Int = -1,
                     var left: Option[Node] = None, var right: Option[Node] = None)
    val binaryTree = Node()
    insert(input, binaryTree)
    def insert(input: Seq[Int], parent: Node): Unit = {
      val middle = input.size/2
      parent.root = input(middle)
      val (left, right) = input.splitAt(middle)
      if (left.size > 0) {
        val leftNode = Node()
        parent.left = Some(leftNode)
        insert(left, leftNode)
      }

      if(right.size > 0) {
        val rightNode = Node()
        parent.right = Some(rightNode)
        insert(right, rightNode)
      }
      insert(right, parent)
    }

    def printInorder(parent: Node): Unit = {
      parent.left.map(printInorder(_))
      print(parent.root)
      parent.right.map(printInorder(_))
    }

  }
*/
  }
