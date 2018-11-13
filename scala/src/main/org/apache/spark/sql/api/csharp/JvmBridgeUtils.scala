/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.sql.api.csharp

import java.util
import org.apache.spark.SparkConf
import scala.collection.JavaConverters._

/*
 * Utils for JvmBridge
 */
object JvmBridgeUtils {
  /*
   * Converts Java HashMap to Scala mutalbe Map
   */
  def toMutableMap[K, V](map: util.HashMap[K, V]) : Map[K, V] = {
    map.asScala.toMap
  }

  def getKeyValuePairAsString(kvp: Tuple2[String, String]) : String = {
    return kvp._1 + "=" + kvp._2
  }

  def getKeyValuePairArrayAsString(kvpArray : Array[Tuple2[String, String]]) : String = {
    val sb = new StringBuilder

    for(kvp <- kvpArray) {
      sb.append(getKeyValuePairAsString(kvp))
      sb.append(";")
    }

    sb.toString
  }

  def getSparkConfAsString(sparkConf: SparkConf): String = {
    getKeyValuePairArrayAsString(sparkConf.getAll)
  }
}
