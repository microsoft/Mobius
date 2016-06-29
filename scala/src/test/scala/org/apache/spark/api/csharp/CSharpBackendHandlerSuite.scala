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
    matchedMethodsStr should not include (".fill(double,java.lang.String[])")
  }

  test("MethodMatchWithNumericParams") {
    class Methods {
      def fill(value: String, cols: Array[String]): Unit = {}

      def fill(value: Long, cols: Array[String]): Unit = {}

      def fill(value: Int, cols: Array[String]): Unit = {}
    }

    val value: Integer = 1
    val params = Array[java.lang.Object](value, Array("col1", "col2"))
    val matchedMethods = new Methods().getClass.getMethods.filter(m => m.getName.startsWith("fill"))
      .filter(m => handler.matchMethod(params.length, params, m.getParameterTypes))

    println("Matched methods:")
    matchedMethods.foreach(println(_))
    //matchedMethods should have length (2)
    matchedMethods should have length (1)

    val matchedMethodsStr = matchedMethods.map(m => m.toString).mkString(" ")

    matchedMethodsStr should not include (".fill(java.lang.String,java.lang.String[])")
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

  test("JVMObjectTrackerTest") {
    val InitMethod = "<init>"
    // bytes offset , keep in accordance with CSharpBackend.scala
    val InitialBytesToStrip = 4
    // in order to test the object release of JvmBridge.CallJavaMethod(true, "SparkCLRHandler", "rm", object-id) : https://github.com/Microsoft/Mobius/blob/master/csharp/Adapter/Microsoft.Spark.CSharp/Interop/Ipc/JvmBridge.cs#L69
    println("JVMObjectTrackerTest : Bytes from : https://github.com/Microsoft/Mobius/blob/master/csharp/AdapterTest/PayloadHelperTest.cs")
    // be careful that release id should not exceed 9 ,or else you should re-generate the bytes from C# : PayloadHelperTest.TestBuildCallBytesForCSharpBackendHandlerSuite()
    val Top9ReleaseBytesHeader = "00-00-00-24-01-00-00-00-0F-53-70-61-72-6B-43-4C-52-48-61-6E-64-6C-65-72-00-00-00-02-72-6D-00-00-00-01-63-00-00-00-01-3"
    val classBytes = List(
      "org.apache.spark.SparkConf" -> "00-00-00-2F-01-00-00-00-1A-6F-72-67-2E-61-70-61-63-68-65-2E-73-70-61-72-6B-2E-53-70-61-72-6B-43-6F-6E-66-00-00-00-06-3C-69-6E-69-74-3E-00-00-00-01-62-01"
    )

    def getBytes(hexString: String): Array[Byte] = {
      hexString.replaceAll("[^0-9A-Fa-f]", "").sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
    }

    def createClass(className: String, hexString: String): Unit = {
      var bytes = getBytes(hexString).drop(InitialBytesToStrip)
      println(s"Try to create object of ${className} with command call bytes[${bytes.length}]")
      handler.handleBackendRequest(bytes)
    }

    for (k <- 0 until classBytes.length) {
      val cb = classBytes(k)
      createClass(cb._1, cb._2)
      val id = k + 1
      val obj = JVMObjectTracker.get(id.toString)
      println(s"Created class : id = ${id} , object = ${obj}")
      assert(obj != None, s"should created object with class name = ${cb._1} , id = ${id}")
      assert(obj.get.getClass.getCanonicalName == cb._1, s"object CanonicalName must be ${cb._1}")
    }

    def releaseObject(id: Integer): Unit = {
      val bytes = getBytes(Top9ReleaseBytesHeader + id.toString).drop(InitialBytesToStrip)
      handler.handleBackendRequest(bytes)
      val obj = JVMObjectTracker.get(id.toString)
      println("Released object : id = " + id + ", now object = " + obj)
      assert(obj == None, s"object with id = ${id} must have been rleased.")
    }

    for (id <- 1 to classBytes.length) {
      releaseObject(id)
    }
  }
}
