/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.api.csharp

import org.apache.spark.util.Utils
import java.io.{DataOutputStream, ByteArrayOutputStream, DataInputStream, ByteArrayInputStream}
import java.net.Socket

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
// TODO - work with SparkR devs to make this configurable and reuse RBackendHandler
import org.apache.spark.api.csharp.SerDe._

import scala.collection.mutable.HashMap

/**
 * Handler for CSharpBackend.
 * This implementation is identical to RBackendHandler and that can be reused
 * in SparkCLR if SerDe is made pluggable
 */
// Since SparkCLR is a package to Spark and not a part of spark-core, it mirrors the implementation
// of selected parts from RBackend with SparkCLR customizations
class CSharpBackendHandler(server: CSharpBackend) extends SimpleChannelInboundHandler[Array[Byte]] {

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    // First bit is isStatic
    val isStatic = readBoolean(dis)
    val objId = readString(dis)
    val methodName = readString(dis)
    val numArgs = readInt(dis)

    if (objId == "SparkCLRHandler") {
      methodName match {
        case "stopBackend" =>
          writeInt(dos, 0)
          writeType(dos, "void")
          server.close()
        case "rm" =>
          try {
            val t = readObjectType(dis)
            assert(t == 'c')
            val objToRemove = readString(dis)
            JVMObjectTracker.remove(objToRemove)
            writeInt(dos, 0)
            writeObject(dos, null)
          } catch {
            case e: Exception =>
              logError(s"Removing $objId failed", e)
              writeInt(dos, -1)
          }
        case "connectCallback" =>
          val t = readObjectType(dis)
          assert(t == 'i')
          val port = readInt(dis)
          // scalastyle:off println
          println("[CSharpBackendHandler] Connecting to a callback server at port " + port)
          CSharpBackend.callbackPort = port
          writeInt(dos, 0)
          writeType(dos, "void")
        case "closeCallback" =>
          // Send close to CSharp callback server.
          println("[CSharpBackendHandler] Requesting to close all call back sockets.")
          // scalastyle:on
          var socket: Socket = null
          do {
            socket = CSharpBackend.callbackSockets.poll()
            if (socket != null) {
              val dataOutputStream = new DataOutputStream(socket.getOutputStream)
              SerDe.writeString(dataOutputStream, "close")
              try {
                socket.close()
                socket = null
              }
            }
          } while (socket != null)
          CSharpBackend.callbackSocketShutdown = true

          writeInt(dos, 0)
          writeType(dos, "void")

        case _ => dos.writeInt(-1)
      }
    } else {
      handleMethodCall(isStatic, objId, methodName, numArgs, dis, dos)
    }

    val reply = bos.toByteArray
    ctx.write(reply)

  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    // Close the connection when an exception is raised.
    // scalastyle:off println
    println("Exception caught: " + cause.getMessage)
    // scalastyle:on
    cause.printStackTrace()
    ctx.close()
  }

  def handleMethodCall(
                        isStatic: Boolean,
                        objId: String,
                        methodName: String,
                        numArgs: Int,
                        dis: DataInputStream,
                        dos: DataOutputStream): Unit = {
    var obj: Object = null
    var args: Array[java.lang.Object] = null
    var methods: Array[java.lang.reflect.Method] = null
    try {
      val cls = if (isStatic) {
        Utils.classForName(objId)
      } else {
        JVMObjectTracker.get(objId) match {
          case None => throw new IllegalArgumentException("Object not found " + objId)
          case Some(o) =>
            obj = o
            o.getClass
        }
      }

      args = readArgs(numArgs, dis)
      methods = cls.getMethods
      val selectedMethods = methods.filter(m => m.getName == methodName)
      if (selectedMethods.length > 0) {
        methods = selectedMethods.filter { x =>
          matchMethod(numArgs, args, x.getParameterTypes)
        }
        if (methods.isEmpty) {
          logWarning(s"cannot find matching method ${cls}.$methodName. "
            + s"Candidates are:")
          selectedMethods.foreach { method =>
            logWarning(s"$methodName(${method.getParameterTypes.mkString(",")})")
          }
          throw new Exception(s"No matched method found for $cls.$methodName")
        }

        val ret = methods.head.invoke(obj, args : _*)

        // Write status bit
        writeInt(dos, 0)
        writeObject(dos, ret.asInstanceOf[AnyRef])
      } else if (methodName == "<init>") {
        // methodName should be "<init>" for constructor
        val ctor = cls.getConstructors.filter { x =>
          matchMethod(numArgs, args, x.getParameterTypes)
        }.head

        val obj = ctor.newInstance(args : _*)

        writeInt(dos, 0)
        writeObject(dos, obj.asInstanceOf[AnyRef])
      } else {
        throw new IllegalArgumentException("invalid method " + methodName + " for object " + objId)
      }
    } catch {
      case e: Exception =>
        // TODO - logError does not work now..fix //logError(s"$methodName on $objId failed", e)
        val jvmObj = JVMObjectTracker.get(objId)
        val jvmObjName = jvmObj match
        {
          case Some(jObj) => jObj.getClass.getName
          case None => "NullObject"
        }
        // scalastyle:off println
        println(s"[CSharpBackendHandler] $methodName on object of type $jvmObjName failed")
        println(e.getMessage)
        println(e.printStackTrace())
        if (methods != null) {
          println("methods:")
          methods.foreach(println(_))
        }
        if (args != null) {
          println("args:")
          args.foreach(arg => {
            if (arg != null) {
              println("argType: " + arg.getClass.getCanonicalName + ", argValue: " + arg)
            } else {
              println("arg: NULL")
            }
          })
        }
        // scalastyle:on println
        writeInt(dos, -1)
        writeString(dos, Utils.exceptionString(e.getCause))
    }
  }

  // Read a number of arguments from the data input stream
  def readArgs(numArgs: Int, dis: DataInputStream): Array[java.lang.Object] = {
    (0 until numArgs).map { arg =>
      readObject(dis)
    }.toArray
  }

  // Checks if the arguments passed in args matches the parameter types.
  // NOTE: Currently we do exact match. We may add type conversions later.
  def matchMethod(
                   numArgs: Int,
                   args: Array[java.lang.Object],
                   parameterTypes: Array[Class[_]]): Boolean = {
    if (parameterTypes.length != numArgs) {
      return false
    }

    for (i <- 0 to numArgs - 1) {
      val parameterType = parameterTypes(i)
      var parameterWrapperType = parameterType

      // Convert native parameters to Object types as args is Array[Object] here
      if (parameterType.isPrimitive) {
        parameterWrapperType = parameterType match {
          case java.lang.Integer.TYPE => classOf[java.lang.Integer]
          case java.lang.Long.TYPE => classOf[java.lang.Long]
          case java.lang.Double.TYPE => classOf[java.lang.Double]
          case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
          case _ => parameterType
        }
      }

      if (!parameterWrapperType.isInstance(args(i))) {
        // non primitive types
        if (!parameterType.isPrimitive && args(i) != null) {
          return false
        }

        // primitive types
        if (parameterType.isPrimitive && !parameterWrapperType.isInstance(args(i))) {
          return false
        }
      }
    }

    true
  }

  // scalastyle:off println
  def logError(id: String) {
    println(id)
  }

  def logWarning(id: String) {
    println(id)
  }
  // scalastyle:on println

  def logError(id: String, e: Exception): Unit = {

  }
}

/**
 * Tracks JVM objects returned to C# which is useful for invoking calls from C# to JVM objects
 */
private object JVMObjectTracker {

  // Muliple threads may access objMap and increase objCounter. Because get method return Option,
  // it is convenient to use a Scala map instead of java.util.concurrent.ConcurrentHashMap.
  private[this] val objMap = new HashMap[String, Object]
  private[this] var objCounter: Int = 1

  def getObject(id: String): Object = {
    synchronized {
      objMap(id)
    }
  }

  def get(id: String): Option[Object] = {
    synchronized {
      objMap.get(id)
    }
  }

  def put(obj: Object): String = {
    synchronized {
      val objId = objCounter.toString
      objCounter = objCounter + 1
      objMap.put(objId, obj)
      objId
    }
  }

  def remove(id: String): Option[Object] = {
    synchronized {
      objMap.remove(id)
    }
  }

}
