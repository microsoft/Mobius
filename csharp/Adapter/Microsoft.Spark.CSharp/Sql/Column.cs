// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Interop;

namespace Microsoft.Spark.CSharp.Sql
{
    public class Column
    {
        private IColumnProxy columnProxy;

        internal IColumnProxy ColumnProxy
        {
            get
            {
                return columnProxy;
            }
        }

        internal Column(IColumnProxy columnProxy)
        {
            this.columnProxy = columnProxy;
        }

        public static implicit operator Column(string name)
        {
            return new Column(CSharpSparkEnvironment.SparkContextProxy.CreateColumnFromName(name));
        }

        public static Column operator !(Column self)
        {
            return new Column(self.columnProxy.FuncOp("not"));
        }

        public static Column operator -(Column self)
        {
            return new Column(self.columnProxy.FuncOp("negate"));
        }

        public static Column operator +(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("plus", other));
        }
        
        public static Column operator -(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("minus", other));
        }

        public static Column operator *(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("multiply", other));
        }

        public static Column operator /(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("divide", other));
        }

        public static Column operator %(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("mod", other));
        }

        public static Column operator ==(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("equalTo", other));
        }

        public static Column operator !=(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("notEqual", other));
        }

        public static Column operator <(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("lt", other));
        }

        public static Column operator <=(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("leq", other));
        }

        public static Column operator >=(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("geq", other));
        }

        public static Column operator >(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("gt", other));
        }

        public static Column operator |(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("bitwiseOR", other));
        }

        public static Column operator &(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("bitwiseAND", other));
        }

        public static Column operator ^(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("bitwiseXOR", other));
        }

        /// <summary>
        /// SQL like expression.
        /// </summary>
        /// <param name="literal"></param>
        /// <returns></returns>
        public Column Like(string literal)
        {
            return new Column(this.columnProxy.BinOp("like", literal));
        }

        /// <summary>
        /// SQL RLIKE expression (LIKE with Regex).
        /// </summary>
        /// <param name="literal"></param>
        /// <returns></returns>
        public Column RLike(string literal)
        {
            return new Column(this.columnProxy.BinOp("rlike", literal));
        }

        /// <summary>
        /// String starts with another string literal.
        /// </summary>
        /// <param name="literal"></param>
        /// <returns></returns>
        public Column StartsWith(Column other)
        {
            return new Column(this.columnProxy.BinOp("startsWith", other));
        }

        /// <summary>
        /// String ends with another string literal.
        /// </summary>
        /// <param name="literal"></param>
        /// <returns></returns>
        public Column EndsWith(Column other)
        {
            return new Column(this.columnProxy.BinOp("endsWith", other));
        }
    }
}
