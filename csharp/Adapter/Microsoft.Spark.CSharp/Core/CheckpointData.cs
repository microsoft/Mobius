// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Container class to store C# checkpoint info.
    /// </summary>
    [Serializable]
    public class CheckpointData
    {
        /// <summary>
        /// Store broadcast variables info. Key is "csharpBid", value is "Value" of the broadcast variable.
        /// </summary>
        public Dictionary<long, object> broadcastVars = new Dictionary<long, object>();

        /// <summary>
        /// Dump checkpoint data to local file
        /// </summary>
        public static void DumpToLocalFile(CheckpointData checkpointData, string path)
        {
            var formatter = new BinaryFormatter();
            using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Write))
            {
                formatter.Serialize(fs, checkpointData);
            }
        }

        /// <summary>
        /// Load checkpoint data to local file
        /// </summary>
        public static CheckpointData LoadFromLocalFile(string path)
        {
            var formatter = new BinaryFormatter();
            using (FileStream fs = new FileStream(path, FileMode.Open, FileAccess.Read))
            {
                return (CheckpointData)formatter.Deserialize(fs);
            }
        }
    }
}
