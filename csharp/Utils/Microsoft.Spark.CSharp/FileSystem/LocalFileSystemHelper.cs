// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Proxy;

namespace Microsoft.Spark.CSharp.Utils
{
    /// <summary>
    /// Helper class that provides basic file system operations for local file system. 
    /// </summary>
    public class LocalFileSystemHelper : IFileSystemHelper
    {
        /// <summary>
        /// Returns an enumerable collection of file names in a specified path.
        /// </summary>
        public IEnumerable<string> EnumerateFiles(string path)
        {
            return Directory.EnumerateFiles(path);
        }

        /// <summary>
        /// Creates a uniquely named, zero-byte temporary file on disk and returns the full path of that file.
        /// </summary>
        public string GetTempFileName()
        {
            return Path.GetTempFileName();
        }

        /// <summary>
        /// Returns the path of the current user's temporary folder.
        /// </summary>
        public string GetTempPath()
        {
            return Path.GetTempPath();
        }

        /// <summary>
        /// Determines whether the given path refers to an existing directory on disk.
        /// </summary>
        public bool Exists(string path)
        {
            return Directory.Exists(path);
        }

        /// <summary>
        /// Deletes the specified directory and, if indicated, any subdirectories and files in the directory.
        /// </summary>
        public bool DeleteDirectory(string path, bool recursive)
        {
            try
            {
                Directory.Delete(path, recursive);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
            
        }

        /// <summary>
        /// Deletes the specified file.
        /// </summary>
        public bool DeleteFile(string path)
        {
            try
            {
                File.Delete(path);
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
