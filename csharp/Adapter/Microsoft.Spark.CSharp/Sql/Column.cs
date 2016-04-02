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
    /// <summary>
    /// A column that will be computed based on the data in a DataFrame.
    /// </summary>
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

        /// <summary>
        /// The logical negation operator that negates its operand.
        /// </summary>
        /// <param name="self">The column self to compute</param>
        /// <returns>true if and only if its operand is false</returns>
        public static Column operator !(Column self)
        {
            return new Column(self.columnProxy.FuncOp("not"));
        }

        /// <summary>
        /// Negation of itself.
        /// </summary>
        /// <param name="self">The column self to compute</param>
        /// <returns>The nagation of itself</returns>
        public static Column operator -(Column self)
        {
            return new Column(self.columnProxy.FuncOp("negate"));
        }

        /// <summary>
        /// Sum of this expression and another expression.
        /// </summary>
        /// <param name="self">The column self to compute</param>
        /// <param name="other">The other object to compute</param>
        /// <returns>The result of sum</returns>
        public static Column operator +(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("plus", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// Subtraction of this expression and another expression.
        /// </summary>
        /// <param name="self">The column self to compute</param>
        /// <param name="other">The other object to compute</param>
        /// <returns>The result of subtraction</returns>
        public static Column operator -(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("minus", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// Multiplication of this expression and another expression.
        /// </summary>
        /// <param name="self">The column self to compute</param>
        /// <param name="other">The other object to compute</param>
        /// <returns>The result of multiplication</returns>
        public static Column operator *(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("multiply", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// Division this expression by another expression.
        /// </summary>
        /// <param name="self">The column self to compute</param>
        /// <param name="other">The other object to compute</param>
        /// <returns>The result of division</returns>
        public static Column operator /(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("divide", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// Modulo (a.k.a. remainder) expression. 
        /// </summary>
        /// <param name="self">The column self to compute</param>
        /// <param name="other">The other object to compute</param>
        /// <returns>The remainder after dividing column self by other</returns>
        public static Column operator %(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("mod", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        ///  The equality operator returns true if the values of its operands are equal, false otherwise.
        /// </summary>
        /// <param name="self">The column self to compare</param>
        /// <param name="other">The other object to compare</param>
        /// <returns>true if the value of self is the same as the value of other; otherwise, false.</returns>
        public static Column operator ==(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("equalTo", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// The inequality operator returns false if its operands are equal, true otherwise.
        /// </summary>
        /// <param name="self">The column self to compare</param>
        /// <param name="other">The other object to compare</param>
        /// <returns>true if the value of self is different from the value of other; otherwise, false.</returns>
        public static Column operator !=(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("notEqual", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// The "less than" relational operator that returns true if the first operand 
        /// is less than the second, false otherwise.
        /// </summary>
        /// <param name="self">The column self to compare</param>
        /// <param name="other">The other object to compare</param>
        /// <returns>true if the value of self is less than the value of other; otherwise, false.</returns>
        public static Column operator <(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("lt", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// The "less than or equal" relational operator that returns true if the first operand 
        /// is less than or equal to the second, false otherwise.
        /// </summary>
        /// <param name="self">The column self to compare</param>
        /// <param name="other">The other object to compare</param>
        /// <returns>true if the value of self is less than or equal to the value of other; otherwise, false.</returns>
        public static Column operator <=(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("leq", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// The "greater than or equal" relational operator that returns true if the first operand 
        /// is greater than or equal to the second, false otherwise.
        /// </summary>
        /// <param name="self">The column self to compare</param>
        /// <param name="other">The other object to compare</param>
        /// <returns>true if the value of self is greater than or equal to the value of other; otherwise, false.</returns>
        public static Column operator >=(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("geq", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// The "greater than" relational operator that returns true if the first operand 
        /// is greater than the second, false otherwise.
        /// </summary>
        /// <param name="self">The column self to compare</param>
        /// <param name="other">The other object to compare</param>
        /// <returns>true if the value of self is greater than the value of other; otherwise, false.</returns>
        public static Column operator >(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("gt", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// Compute bitwise OR of this expression with another expression.
        /// </summary>
        /// <param name="self">The column self to compute</param>
        /// <param name="other">The other object to compute</param>
        /// <returns>false if and only if both its operands are false; otherwise, true</returns>
        public static Column operator |(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("bitwiseOR", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// Compute bitwise AND of this expression with another expression.
        /// </summary>
        /// <param name="self">The column self to compute</param>
        /// <param name="other">The other object to compute</param>
        /// <returns>true if and only if both its operands are true; otherwise, false</returns>
        public static Column operator &(Column self, object other)
        {
            return new Column(self.columnProxy.BinOp("bitwiseAND", (other is Column) ? ((Column)other).columnProxy : other));
        }

        /// <summary>
        /// Compute bitwise XOR of this expression with another expression.
        /// </summary>
        /// <param name="self">The column self to compute</param>
        /// <param name="other">The other object to compute</param>
        /// <returns>true if and only if exactly one of its operands is true; otherwise, false</returns>
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

        /// <summary>
        /// Returns a sort expression based on the ascending order.
        /// </summary>
        /// <returns>A column with ascending order</returns>
        public Column Asc()
        {
            return new Column(columnProxy.UnaryOp("asc"));            
        }

        /// <summary>
        /// Returns a sort expression based on the descending order.
        /// </summary>
        /// <returns>A column with descending order</returns>
        public Column Desc()
        {
            return new Column(columnProxy.UnaryOp("desc"));
        }

        /// <summary>
        /// Returns this column aliased with a new name.
        /// </summary>
        /// <param name="alias">The name of alias</param>
        /// <returns>A column aliased with the given name</returns>
        public Column Alias(string alias)
        {
            return new Column(columnProxy.InvokeMethod("as", alias));
        }

        /// <summary>
        /// Returns this column aliased with new names
        /// </summary>
        /// <param name="aliases">The array of names for aliases</param>
        /// <returns>A column aliased with the given names</returns>
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
