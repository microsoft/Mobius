// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

namespace Microsoft.Spark.CSharp.Sql
{
    /// <summary>
    /// SaveMode is used to specify the expected behavior of saving a DataFrame to a data source.
    /// </summary>
    public enum SaveMode
    {
        /// <summary>
        /// Append mode means that when saving a DataFrame to a data source, if data/table already exists,
        /// contents of the DataFrame are expected to be appended to existing data.
        /// </summary>
        Append,

        /// <summary>
        /// Overwrite mode means that when saving a DataFrame to a data source,
        /// if data/table already exists, existing data is expected to be overwritten by the contents of
        /// the DataFrame.
        /// </summary>
        Overwrite,

        /// <summary>
        /// ErrorIfExists mode means that when saving a DataFrame to a data source, if data already exists,
        /// an exception is expected to be thrown.
        /// </summary>
        ErrorIfExists,

        /// <summary>
        /// Ignore mode means that when saving a DataFrame to a data source, if data already exists,
        /// the save operation is expected to not save the contents of the DataFrame and to not
        /// change the existing data.
        /// </summary>
        Ignore
    }

    /// <summary>
    /// For SaveMode.ErrorIfExists, the corresponding literal string in spark is "error" or "default".
    /// </summary>
    public static class SaveModeExtensions
    {
        public static string GetStringValue(this SaveMode mode)
        {
            switch (mode)
            {
                case SaveMode.ErrorIfExists:
                    return "error";
                default:
                    return mode.ToString();
            }
        }
    }
}
