/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package org.apache.spark.api.csharp

import org.apache.spark.csharp.SparkCLRFunSuite
import org.scalatest._

class CSharpBackendHandlerSuite extends SparkCLRFunSuite with Matchers {
  private val handler = new CSharpBackendHandler(null)

  test("MethodMatch") {
    class Methods {
      def fill(value: Double, cols: Array[String]): Unit = {}

      def fill(value: String, cols: Array[String]): Unit = {}
    }

    val params = Array[java.lang.Object]("unknown value", Array("col1", "col2"))
    val matchedMethods = new Methods().getClass.getMethods.filter(m => m.getName.startsWith("fill"))
      .filter(m => handler.matchMethod(params.length, params, m.getParameterTypes))

    println("Matched methods:")
    matchedMethods.foreach(println(_))
    matchedMethods should have length (1)

    val matchedMethodsStr = matchedMethods.map(m => m.toString).mkString(" ")
    matchedMethodsStr should include(".fill(java.lang.String,java.lang.String[])")
    matchedMethodsStr should not include(".fill(double,java.lang.String[])")
  }

  test("MethodMatchWithNumericParams") {
    class Methods {
      def fill(value: String, cols: Array[String]): Unit = {}

      def fill(value: Long, cols: Array[String]): Unit = {}

      def fill(value: Int, cols: Array[String]): Unit = {}
    }

    val value:Integer = 1
    val params = Array[java.lang.Object](value, Array("col1", "col2"))
    val matchedMethods = new Methods().getClass.getMethods.filter(m => m.getName.startsWith("fill"))
      .filter(m => handler.matchMethod(params.length, params, m.getParameterTypes))

    println("Matched methods:")
    matchedMethods.foreach(println(_))
    //matchedMethods should have length (2)
    matchedMethods should have length (1)

    val matchedMethodsStr = matchedMethods.map(m => m.toString).mkString(" ")

    matchedMethodsStr should not include(".fill(java.lang.String,java.lang.String[])")
    //matchedMethodsStr should include(".fill(long,java.lang.String[])")
    matchedMethodsStr should include(".fill(int,java.lang.String[])")
  }

  test("ConstructorsMatch") {
    // no match
    var params = Array[java.lang.Object](new java.lang.Integer(1), new Integer(1))
    var matchedConstructors = classOf[org.apache.spark.api.python.PythonPartitioner].getClass.
      getConstructors.filter(c => handler.matchMethod(params.length, params, c.getParameterTypes))
    matchedConstructors should have length (0)

    // match 1
    params = Array[java.lang.Object](new java.lang.Integer(1), new java.lang.Long(1))
    matchedConstructors = classOf[org.apache.spark.api.python.PythonPartitioner].getConstructors
      .filter(c => handler.matchMethod(params.length, params, c.getParameterTypes))

    println("Matched constructors:")
    matchedConstructors.foreach(println(_))
    matchedConstructors should have length (1)
    val matchedMethodsStr = matchedConstructors.map(m => m.toString).mkString(" ")
    matchedMethodsStr should include("org.apache.spark.api.python.PythonPartitioner(int,long)")
  }
}
