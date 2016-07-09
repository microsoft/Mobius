/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.streaming.api.csharp

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.{JavaDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.DStream

/*
 * Utils for EventHubs
 */
object EventHubsUtils {
  def createUnionStream (
                          jssc: JavaStreamingContext,
                          eventhubsParams: Map[String, String],
                          storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                        ): JavaDStream[Array[Byte]] = {
    // using reflection to avoid taking dependency on EventHubs jar - it is not available in Maven yet
    val className = "org.apache.spark.streaming.eventhubs.EventHubsUtils"
    val dstream = org.apache.spark.util.Utils.classForName(className)
                                              .getMethods
                                              .filter(m => m.getName == "createUnionStream")
                                              .head
                                              .invoke(null, jssc.ssc, eventhubsParams, storageLevel)
                                              .asInstanceOf[DStream[Array[Byte]]]
    JavaDStream.fromDStream(dstream)
  }
}
