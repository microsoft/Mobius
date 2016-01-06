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
        private readonly IColumnProxy columnProxy;

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
            return new Column(self.columnProxy.BinOp("plus", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator -(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("minus", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator *(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("multiply", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator /(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("divide", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator %(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("mod", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator ==(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("equalTo", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator !=(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("notEqual", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator <(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("lt", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator <=(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("leq", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator >=(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("geq", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator >(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("gt", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator |(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("bitwiseOR", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator &(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("bitwiseAND", (other is Column) ? ((Column)other).columnProxy : other));
        }

        public static Column operator ^(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("bitwiseXOR", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// Required when operator == or operator != is defined
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return (columnProxy != null ? columnProxy.GetHashCode() : 0);
        }

        /// <summary>
        /// Required when operator == or operator != is defined
        /// </summary>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            return obj.GetType() == GetType() && Equals(columnProxy, ((Column)obj).columnProxy);
        }

        /// <summary>
        /// SQL like expression.
        /// </summary>
        /// <param name="literal"></param>
        /// <returns></returns>
        public Column Like(string literal)
        {
            return new Column(columnProxy.BinOp("like", literal));
        }

        /// <summary>
        /// SQL RLIKE expression (LIKE with Regex).
        /// </summary>
        /// <param name="literal"></param>
        /// <returns></returns>
        public Column RLike(string literal)
        {
            return new Column(columnProxy.BinOp("rlike", literal));
        }

        /// <summary>
        /// String starts with another string literal.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public Column StartsWith(Column other)
        {
            return new Column(columnProxy.BinOp("startsWith", other.columnProxy));
        }

        /// <summary>
        /// String ends with another string literal.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public Column EndsWith(Column other)
        {
            return new Column(columnProxy.BinOp("endsWith", other.columnProxy));
        }

        public Column Asc()
        {
            return new Column(columnProxy.UnaryOp("asc"));            
        }
        public Column Desc()
        {
            return new Column(columnProxy.UnaryOp("desc"));
        }

        public Column Alias(string alias)
        {
            return new Column(columnProxy.InvokeMethod("as", alias));
        }

        public Column Alias(string[] aliases)
        {
            return new Column(columnProxy.InvokeMethod("as", new object[] { aliases }));
        }

        /// <summary>
        /// Casts the column to a different data type, using the canonical string representation
        /// of the type. The supported types are: `string`, `boolean`, `byte`, `short`, `int`, `long`,
        /// `float`, `double`, `decimal`, `date`, `timestamp`.
        /// 
        /// E.g.
        ///     // Casts colA to integer.
        ///     df.select(df("colA").cast("int"))
        /// </summary>
        public Column Cast(string to)
        {
            return new Column(columnProxy.InvokeMethod("cast", new object[] { to }));
        }
    }
}
