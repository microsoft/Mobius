/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package org.apache.spark.deploy.csharp

import org.apache.commons.lang3.SystemUtils
import org.apache.spark.csharp.SparkCLRFunSuite

class CSharpRunnerSuite extends SparkCLRFunSuite {
  test("formatPath") {
    if (SystemUtils.IS_OS_WINDOWS) {
      // no change to absolute Windows path
      val path1 =
        """c:\path\to\Mobius\application.exe"""
      assert(path1.equals(CSharpRunner.formatPath(path1)))
    } else {
      // no change to absolute Linux path
      val path2 =
        """/path/to/Mobius/application.sh.exe"""
      assert(path2.equals(CSharpRunner.formatPath(path2)))
    }

    // non-absolute, single-part name is formatted in Windows and Linux
    val path3 = """application.sh.exe"""
    assert(CSharpRunner.formatPath(path3).startsWith("."))
    assert(CSharpRunner.formatPath(path3).endsWith(path3))
  }
}
