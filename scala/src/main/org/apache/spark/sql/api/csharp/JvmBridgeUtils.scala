/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.sql.api.csharp

import java.util
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
}
