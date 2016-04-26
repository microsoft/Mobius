// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// Container for an optional value of type T. If the value of type T is present, the Option.IsDefined is TRUE and GetValue() return the value. 
    /// If the value is absent, the Option.IsDefined is FALSE, exception will be thrown when calling GetValue().
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [Serializable]
    public class Option<T>
    {
        private bool isDefined = false;
        private T value;

        /// <summary>
        /// Initialize a instance of Option class without any value.
        /// </summary>
        public Option()
        { }

        /// <summary>
        /// Initializes a instance of Option class with a specific value. 
        /// </summary>
        /// <param name="value">The value to be associated with the new instance.</param>
        public Option(T value)
        {
            isDefined = true;
            this.value = value;
        }

        /// <summary>
        /// Indicates whether the option value is defined.
        /// </summary>
        public bool IsDefined { get { return isDefined; } }

        /// <summary>
        /// Returns the value of the option if Option.IsDefined is TRUE;
        /// otherwise, throws an <see cref="ArgumentException"/>.
        /// </summary>
        /// <returns></returns>
        public T GetValue()
        {
            if (isDefined) return value;

            throw new ArgumentException("Value is not defined.");
        }
    }
}
