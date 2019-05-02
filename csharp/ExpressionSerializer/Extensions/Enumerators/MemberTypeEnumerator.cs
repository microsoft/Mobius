using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Extensions.Enumerators
{
    internal class MemberTypeEnumerator : IEnumerator<Type>
    {
        private int _currentIndex;
        private readonly Type _type;
        private readonly BindingFlags _bindingFlags;
        private readonly HashSet<Type> _seenTypes;
        private Type[] _allTypes;

        /// <summary>
        /// Initializes a new instance of the <see cref="MemberTypeEnumerator"/> class.
        /// </summary>
        /// <param name="seenTypes">The seen types.</param>
        /// <param name="type">The type.</param>
        /// <param name="bindingFlags">The binding flags.</param>
        /// <exception cref="System.ArgumentNullException">
        /// seenTypes
        /// or
        /// type
        /// </exception>
        public MemberTypeEnumerator(HashSet<Type> seenTypes, Type type, BindingFlags bindingFlags)
        {
            _seenTypes = seenTypes ?? throw new ArgumentNullException(nameof(seenTypes));
            _type = type ?? throw new ArgumentNullException(nameof(type));
            _bindingFlags = bindingFlags;

            _currentIndex = -1;
        }

        /// <summary>
        /// Gets a value indicating whether this instance is considered.
        /// </summary>
        /// <value>
        /// <c>true</c> if this instance is considered; otherwise, <c>false</c>.
        /// </value>
        public bool IsConsidered => this.IsConsideredType(_type);

        /// <summary>
        /// Determines whether [is considered type] [the specified type].
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if [is considered type] [the specified type]; otherwise, <c>false</c>.
        /// </returns>
        protected virtual bool IsConsideredType(Type type)
        {
            return true;
        }

        /// <summary>
        /// Determines whether [is considered member] [the specified member].
        /// </summary>
        /// <param name="member">The member.</param>
        /// <returns>
        ///   <c>true</c> if [is considered member] [the specified member]; otherwise, <c>false</c>.
        /// </returns>
        protected virtual bool IsConsideredMember(MemberInfo member)
        {
            return true;
        }

        /// <summary>
        /// Determines whether [is seen type] [the specified type].
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns>
        ///   <c>true</c> if [is seen type] [the specified type]; otherwise, <c>false</c>.
        /// </returns>
        protected bool IsSeenType(Type type)
        {
            return _seenTypes.Contains(type);
        }

        /// <summary>
        /// Adds the type of the seen.
        /// </summary>
        /// <param name="type">The type.</param>
        protected void AddSeenType(Type type)
        {
            _seenTypes.Add(type);
        }

        /// <summary>
        /// Gets the current.
        /// </summary>
        /// <value>
        /// The current.
        /// </value>
        public virtual Type Current => _allTypes[_currentIndex];

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public void Dispose() { }

        /// <summary>
        /// Gets the current.
        /// </summary>
        /// <value>
        /// The current.
        /// </value>
        object System.Collections.IEnumerator.Current => this.Current;

        /// <summary>
        /// Gets the type of the types of.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        protected Type[] GetTypesOfType(Type type)
        {
            var types = new List<Type> { type };
            if (type.HasElementType)
                types.AddRange(this.GetTypesOfType(type.GetElementType()));
            if (type.GetType().IsGenericType)
            {
                foreach (var genericType in type.GetGenericArguments())
                    types.AddRange(this.GetTypesOfType(genericType));

            }
            return types.ToArray();
        }

        /// <summary>
        /// Builds the types.
        /// </summary>
        /// <returns></returns>
        protected virtual Type[] BuildTypes()
        {
            var types = new List<Type>();
            var members = _type.GetMembers(_bindingFlags);
            foreach (var memberInfo in members.Where(this.IsConsideredMember))
                types.AddRange(this.GetTypesOfType(GetReturnType(memberInfo)));
            return types.ToArray();
        }

        private Type GetReturnType(MemberInfo member)
        {
            switch (member)
            {
                case PropertyInfo propertyInfo:
                    return propertyInfo.PropertyType;
                case MethodInfo methodInfo:
                    return methodInfo.ReturnType;
                case FieldInfo fieldInfo:
                    return fieldInfo.FieldType;
            }

            throw new NotSupportedException("Unable to get return type of member of type " + member.GetType().Name);
        }

        /// <summary>
        /// Advances the enumerator to the next element of the collection.
        /// </summary>
        /// <returns>
        /// true if the enumerator was successfully advanced to the next element; false if the enumerator has passed the end of the collection.
        /// </returns>
        public virtual bool MoveNext()
        {
            if (!this.IsConsidered)
                return false;

            if (_allTypes == null)
                _allTypes = this.BuildTypes();

            while (++_currentIndex < _allTypes.Length)
            {
                if (this.IsSeenType(this.Current)) continue;
                this.AddSeenType(this.Current);
                if (this.IsConsideredType(this.Current)) break;
            }

            return _currentIndex < _allTypes.Length;
        }

        /// <summary>
        /// Sets the enumerator to its initial position, which is before the first element in the collection.
        /// </summary>
        public void Reset()
        {
            _currentIndex = -1;
        }
    }
}
