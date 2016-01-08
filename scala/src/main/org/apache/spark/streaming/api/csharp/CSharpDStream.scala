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

package org.apache.spark.streaming.api.csharp

import org.apache.spark.api.csharp._
import org.apache.spark.api.csharp.SerDe._

import java.io.DataInputStream
import java.io.DataOutputStream
import java.net.Socket
import java.util.{ArrayList => JArrayList}
import scala.collection.JavaConversions._
import scala.language.existentials

import org.apache.spark.api.java._
import org.apache.spark.rdd._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Duration, Interval, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._

import scala.language.existentials

object CSharpDStream {

  /**
   * helper function for DStream.foreachRDD().
   */
  def callForeachRDD(jdstream: JavaDStream[Array[Byte]], cSharpFunc: Array[Byte],
                     serializationMode: String) {
    val func = (rdd: RDD[_], time: Time) => {
      val res = callCSharpTransform(List(Some(rdd)), time, cSharpFunc, List(serializationMode))
    }
    jdstream.dstream.foreachRDD(func)
  }

  def callCSharpTransform(rdds: List[Option[RDD[_]]], time: Time, cSharpfunc: Array[Byte],
                          serializationModeList: List[String]): Option[RDD[Array[Byte]]] = {
    var socket: Socket = null
    try {
      socket = CSharpBackend.callbackSockets.poll()
      if (socket == null) {
        socket = new Socket("localhost", CSharpBackend.callbackPort)
      }

      val dos = new DataOutputStream(socket.getOutputStream())
      val dis = new DataInputStream(socket.getInputStream())

      writeString(dos, "callback")
      writeInt(dos, rdds.size)
      rdds.foreach(x => writeObject(dos,
        x.map(JavaRDD.fromRDD(_).asInstanceOf[AnyRef]).orNull))
      writeDouble(dos, time.milliseconds.toDouble)
      writeBytes(dos, cSharpfunc)
      serializationModeList.foreach(x => writeString(dos, x))
      dos.flush()
      val result = Option(readObject(dis).asInstanceOf[JavaRDD[Array[Byte]]]).map(_.rdd)
      CSharpBackend.callbackSockets.offer(socket)
      result
    } catch {
      case e: Exception =>
        // log exception only when callback socket is not shutdown explicitly
        if (!CSharpBackend.callbackSocketShutdown) {
          // TODO: change println to log
          System.err.println("CSharp transform callback failed with " + e) // scalastyle:off println
          e.printStackTrace()
        }

        // close this socket if error happen
        if (socket != null) {
          try {
            socket.close()
          }
        }

        None
    }
  }

  /**
   * convert list of RDD into queue of RDDs, for ssc.queueStream()
   */
  def toRDDQueue(rdds: JArrayList[JavaRDD[Array[Byte]]]): java.util.Queue[JavaRDD[Array[Byte]]] = {
    val queue = new java.util.LinkedList[JavaRDD[Array[Byte]]]
    rdds.forall(queue.add(_))
    queue
  }
}

class CSharpDStream(
                     parent: DStream[_],
                     cSharpFunc: Array[Byte],
                     serializationMode: String)
  extends DStream[Array[Byte]] (parent.ssc) {

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val rdd = parent.compute(validTime)
    if (rdd.isDefined) {
      CSharpDStream.callCSharpTransform(List(rdd), validTime, cSharpFunc, List(serializationMode))
    } else {
      None
    }
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * Transformed from two DStreams in CSharp
 */
class CSharpTransformed2DStream(
                                 parent: DStream[_],
                                 parent2: DStream[_],
                                 cSharpFunc: Array[Byte],
                                 serializationMode: String,
                                 serializatioinMode2: String)
  extends DStream[Array[Byte]] (parent.ssc) {

  override def dependencies: List[DStream[_]] = List(parent, parent2)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val empty: RDD[_] = ssc.sparkContext.emptyRDD
    val rdd1 = Some(parent.getOrCompute(validTime).getOrElse(empty))
    val rdd2 = Some(parent2.getOrCompute(validTime).getOrElse(empty))
    CSharpDStream.callCSharpTransform(List(rdd1, rdd2), validTime, cSharpFunc,
      List(serializationMode, serializatioinMode2))
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * similar to ReducedWindowedDStream
 */
class CSharpReducedWindowedDStream(
                                    parent: DStream[Array[Byte]],
                                    rreduceFunc: Array[Byte],
                                    rinvReduceFunc: Array[Byte],
                                    _windowDuration: Duration,
                                    _slideDuration: Duration,
                                    serializationMode: String)
  extends DStream[Array[Byte]] (parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY)

  override def dependencies: List[DStream[_]] = List(parent)

  override val mustCheckpoint: Boolean = true

  def windowDuration: Duration = _windowDuration

  override def slideDuration: Duration = _slideDuration

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val currentTime = validTime
    val current = new Interval(currentTime - windowDuration, currentTime)
    val previous = current - slideDuration

    //  _____________________________
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    //                     |_____________________________|
    //
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs
    //
    val previousRDD = getOrCompute(previous.endTime)

    // for small window, reduce once will be better than twice
    if (rinvReduceFunc != null && previousRDD.isDefined
      && windowDuration >= slideDuration * 5) {

      // subtract the values from old RDDs
      val oldRDDs = parent.slice(previous.beginTime + parent.slideDuration, current.beginTime)
      val subtracted = if (oldRDDs.size > 0) {
        CSharpDStream.callCSharpTransform(List(previousRDD, Some(ssc.sc.union(oldRDDs))),
          validTime, rinvReduceFunc, List(serializationMode, serializationMode))
      } else {
        previousRDD
      }

      // add the RDDs of the reduced values in "new time steps"
      val newRDDs = parent.slice(previous.endTime + parent.slideDuration, current.endTime)
      if (newRDDs.size > 0) {
        CSharpDStream.callCSharpTransform(List(subtracted, Some(ssc.sc.union(newRDDs))),
          validTime, rreduceFunc, List(serializationMode, serializationMode))
      } else {
        subtracted
      }
    } else {
      // Get the RDDs of the reduced values in current window
      val currentRDDs = parent.slice(current.beginTime + parent.slideDuration, current.endTime)
      if (currentRDDs.size > 0) {
        CSharpDStream.callCSharpTransform(List(None, Some(ssc.sc.union(currentRDDs))),
          validTime, rreduceFunc, List(serializationMode, serializationMode))
      } else {
        None
      }
    }
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * similar to StateDStream
 */
class CSharpStateDStream(
                          parent: DStream[Array[Byte]],
                          reduceFunc: Array[Byte],
                          serializationMode: String,
                          serializationMode2: String)
  extends DStream[Array[Byte]](parent.ssc) {

  super.persist(StorageLevel.MEMORY_ONLY)

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val lastState = getOrCompute(validTime - slideDuration)
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isDefined) {
      CSharpDStream.callCSharpTransform(List(lastState, rdd), validTime, reduceFunc,
        List(serializationMode, serializationMode2))
    } else {
      lastState
    }
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

