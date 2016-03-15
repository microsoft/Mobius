// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Utils
{
    /// <summary>
    /// Helper class that provides basic file system operations.
    /// </summary>
    public interface IFileSystemHelper
    {
        /// <summary>
        /// Returns an enumerable collection of file names in a specified path.
        /// </summary>
        IEnumerable<string> EnumerateFiles(string path);

        /// <summary>
        /// Build a uniquely named temp file and return the path.
        /// </summary>
        string GetTempFileName();

        /// <summary>
        /// Returns the path of the current user's or system temporary folder.
        /// </summary>
        string GetTempPath();

        /// <summary>
        /// Determines whether the given path exists.
        /// </summary>
        bool Exists(string path);

        /// <summary>
        /// Deletes the specified directory and, if indicated, any subdirectories and files in the directory.
        /// </summary>
        bool DeleteDirectory(string path, bool recursive);

        /// <summary>
        /// Deletes the specified file.
        /// </summary>
        bool DeleteFile(string path);
    }
}
