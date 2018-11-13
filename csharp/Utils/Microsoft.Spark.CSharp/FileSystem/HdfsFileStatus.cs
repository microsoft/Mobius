// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Proxy.Ipc;

namespace Microsoft.Spark.CSharp.Utils.FileSystem
{
	/// <summary>
	/// See https://hadoop.apache.org/docs/r2.6.1/api/org/apache/hadoop/fs/FileStatus.html
	/// </summary>
	public class HdfsFileStatus
	{
		public long Length => _status.Value.Length;
		public long ModificationTime => _status.Value.Time;
		public string Owner => _status.Value.Owner;
		public string Path => _status.Value.Path;
		public bool IsFile => _status.Value.IsFile;
		public bool IsDirectory => _status.Value.IsDirectory;
		public bool IsSymlink => _status.Value.IsSymlink;

		private Lazy<Status> _status;

		internal HdfsFileStatus(JvmObjectReference obj)
		{
			_status = new Lazy<Status>(()=>new Status(obj));
		}

		private class Status
		{
			public long Length;
			public long Time;
			public string Owner;
			public string Path;
			public bool IsFile;
			public bool IsDirectory;
			public bool IsSymlink;

			public Status(JvmObjectReference obj)
			{
				Length = (long) SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(obj, "getLen");
				Time = (long)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(obj, "getModificationTime");
				Owner = (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(obj, "getOwner");
				IsFile = (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(obj, "isFile");
				IsDirectory = (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(obj, "isDirectory");
				IsSymlink = (bool)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(obj, "isSymlink");
				var pr = new JvmObjectReference((string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(obj, "getPath"));
				Path = (string)SparkCLRIpcProxy.JvmBridge.CallNonStaticJavaMethod(pr, "getName");
			}
		}
	}
}
