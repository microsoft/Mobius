/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.util.csharp

import org.apache.spark.SparkConf
import org.apache.spark.csharp.SparkCLRFunSuite
import org.apache.spark.sql.api.csharp.JvmBridgeUtils


class JvmBridgeUtilsSuite extends SparkCLRFunSuite{
  test("getSparkConfAsString") {
    var sparkConf = new SparkConf(true)
    val appName = "appName"
    sparkConf.setAppName(appName)
    val master = "master"
    sparkConf.setMaster(master)
    val kvp1 = ("spark.config1.name", "config1.value")
    sparkConf.set(kvp1._1, kvp1._2)
    val kvp2 = ("spark.config2.name", "config2.value")
    sparkConf.set(kvp2._1, kvp2._2)

    val returnValue = JvmBridgeUtils.getSparkConfAsString(sparkConf)
    assert(returnValue.contains(s"spark.master=${master}"))
    assert(returnValue.contains(s"spark.app.name=${appName}"))
    assert(returnValue.contains(s"${kvp1._1}=${kvp1._2}"))
    assert(returnValue.contains(s"${kvp2._1}=${kvp2._2}"))
  }
}