using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Extensions.Enumerators
{
    internal class ComplexPropertyMemberTypeEnumerator : PropertyMemberTypeEnumerator
    {
        private static readonly Type[] _builtinTypes;

        /// <summary>
        /// Initializes the <see cref="ComplexPropertyMemberTypeEnumerator"/> class.
        /// </summary>
        static ComplexPropertyMemberTypeEnumerator()
        {
            _builtinTypes = new[] { typeof(bool), typeof(byte), typeof(sbyte), typeof(char), typeof(decimal), typeof(double), typeof(float),
                typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(object), typeof(short), typeof(ushort), typeof(string),
                typeof(Guid), typeof(Int16),typeof(Int32),typeof(Int64), typeof(UInt16), typeof(UInt32), typeof(UInt64), typeof(TimeSpan), typeof(DateTime) };
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComplexPropertyMemberTypeEnumerator"/> class.
        /// </summary>C:\Dev\Esskar\Serialize.Linq\src\Serialize.Linq\Internals\MemberTypeEnumerator.cs
        /// <param name="type">The type.</param>
        /// <param name="bindingFlags">The binding flags.</param>
        public ComplexPropertyMemberTypeEnumerator(Type type, BindingFlags bindingFlags)
            : this(new HashSet<Type>(), type, bindingFlags) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComplexPropertyMemberTypeEnumerator"/> class.
        /// </summary>
        /// <param name="seenTypes">The seen types.</param>
        /// <param name="type">The type.</param>
        /// <param name="bindingFlags">The binding flags.</param>
        public ComplexPropertyMemberTypeEnumerator(HashSet<Type> seenTypes, Type type, BindingFlags bindingFlags)
            : base(seenTypes, type, bindingFlags) { }

        /// <summary>
        /// Determines whether [is builtin type] [the specified type].
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if [is builtin type] [the specified type]; otherwise, <c>false</c>.
        /// </returns>
        private static bool IsBuiltinType(Type type)
        {
            return _builtinTypes.Contains(type);
        }

        /// <summary>
        /// Determines whether [is considered type] [the specified type].
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if [is considered type] [the specified type]; otherwise, <c>false</c>.
        /// </returns>
        protected override bool IsConsideredType(Type type)
        {
            return !ComplexPropertyMemberTypeEnumerator.IsBuiltinType(type)
                && base.IsConsideredType(type);
        }
    }
}
