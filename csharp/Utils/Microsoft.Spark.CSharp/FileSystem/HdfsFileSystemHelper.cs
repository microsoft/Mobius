// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Microsoft.Spark.CSharp.Interop;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy.Ipc;
using Microsoft.Spark.CSharp.Utils.FileSystem;

namespace Microsoft.Spark.CSharp.Utils
{
    /// <summary>
    /// Helper class that provides basic file system operations for HDFS.
    /// </summary>
    public class HdfsFileSystemHelper : IFileSystemHelper
    {
        private readonly JvmObjectReference jvmHdfsReference;

        public HdfsFileSystemHelper()
        {
	        var jvmConfReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.conf.Configuration");
            jvmHdfsReference = new JvmObjectReference((string) SparkCLRIpcProxy.JvmBridge.CallStaticJavaMethod("org.apache.hadoop.fs.FileSystem", "get", jvmConfReference));
        }

        /// <summary>
        /// List the names of all the files under the given path.
        /// </summary>
        public IEnumerable<string> EnumerateFiles(string path)
        {
            var pathJvmReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);
            var statusList = (List<JvmObjectReference>)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHdfsReference, "listStatus", pathJvmReference);
            if (statusList == null || statusList.Count == 0)
            {
                return new string[0];
            }

            var files = new string[statusList.Count];

            for (var i = 0; i < statusList.Count; i++)
            {
                var subPathJvmReference = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(statusList[i], "getPath"));
                files[i] = (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(subPathJvmReference, "getName");	
            }

            return files;
        }

		/// <summary>
		/// List the names of all the files under the given path.
		/// </summary>
		public IEnumerable<HdfsFileStatus> ListStatus(string path)
		{
			var pathJvmReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);
			return ((List<JvmObjectReference>)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHdfsReference, "listStatus", pathJvmReference)).Select(r=>new HdfsFileStatus(r));
		}

		/// <summary>
		/// Build a temp file path under '/tmp' path on HDFS.
		/// </summary>
		public string GetTempFileName()
        {
            return "/tmp/" + Guid.NewGuid().ToString("N");
        }

        /// <summary>
        /// Get the temp path on HDFS.
        /// </summary>
        public string GetTempPath()
        {
            return "/tmp/";
        }

        /// <summary>
        /// Check whether the given path exists on HDFS.
        /// </summary>
        public bool Exists(string path)
        {
            var pathJvmReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);
            return (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHdfsReference, "exists", pathJvmReference);
        }

        /// <summary>
        /// Deletes the specified directory and, if indicated, any subdirectories and files in the directory.
        /// </summary>
        public bool DeleteDirectory(string path, bool recursive)
        {
            return Delete(path, true);
        }

        /// <summary>
        /// Deletes the specified path.
        /// </summary>
        public bool DeleteFile(string path)
        {
            return Delete(path, false);
        }

        internal bool Delete(string path, bool recursive)
        {
            var pathJvmReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);
            return (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHdfsReference, "delete", pathJvmReference, recursive);
        }

	    public bool IsFile(string path)
	    {
			var pathJvmReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);
			return (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHdfsReference, "isFile", pathJvmReference);
		}

		public bool IsDirectory(string path)
		{
			var pathJvmReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);
			return (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHdfsReference, "isDirectory", pathJvmReference);
		}

		public bool Touch(string path)
		{
			var pathJvmReference = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", path);
			return (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHdfsReference, "createNewFile", pathJvmReference);
		}

		public void CopyFromLocalFile(string src, string dest)
		{
			var from = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", new Uri(src).AbsoluteUri);
			var to = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", dest);
			SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHdfsReference, "copyFromLocalFile", from, to);
		}

		public void CopyToLocalFile(string src, string dest)
		{
			var to = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", new Uri(dest).AbsoluteUri);
			var from = SparkCLRIpcProxy.JvmBridge.CallConstructor("org.apache.hadoop.fs.Path", src);
			SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(jvmHdfsReference, "copyToLocalFile", from, to);
		}
	}
}
