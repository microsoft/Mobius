// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Text.RegularExpressions;

namespace Microsoft.Spark.CSharp.Samples
{
    /// <summary>
    /// Attribute that marks a method as a sample
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    internal class SampleAttribute : Attribute
    {
        public const string CATEGORY_ALL = "all";   // run all sample tests
        public const string CATEGORY_DEFAULT = "default"; // run default tests

        private readonly string category;

        public SampleAttribute(string category)
        {
            this.category = category;
        }

        public SampleAttribute()
        {
            category = CATEGORY_DEFAULT;
        }

        public string Category
        {
            get
            {
                return category;
            }
        }

        /// <summary>
        /// whether this category matches the target category
        /// </summary>
        // public bool Match(string targetCategory)
        public bool Match(Regex targetCategory)
        {
            if (null == targetCategory)
            {
                throw new ArgumentNullException("targetCategory");
            }

            return targetCategory.IsMatch(CATEGORY_ALL)
                || targetCategory.IsMatch(category);
        }

        public override string ToString()
        {
            return category;
        }
    }
}
