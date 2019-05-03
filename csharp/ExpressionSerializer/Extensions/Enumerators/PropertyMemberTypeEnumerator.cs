using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Extensions.Enumerators
{
    internal class PropertyMemberTypeEnumerator : MemberTypeEnumerator
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PropertyMemberTypeEnumerator"/> class.
        /// </summary>
        /// <param name="seenTypes">The seen types.</param>
        /// <param name="type">The type.</param>
        /// <param name="bindingFlags">The binding flags.</param>
        public PropertyMemberTypeEnumerator(HashSet<Type> seenTypes, Type type, BindingFlags bindingFlags)
            : base(seenTypes, type, bindingFlags
#if  !WINDOWS_UWP && !NETSTANDARD
                  | BindingFlags.SetProperty | BindingFlags.GetProperty
#endif
                  )
        { }

        /// <summary>
        /// Determines whether the specified member is to be considered.
        /// </summary>
        /// <param name="member">The member.</param>
        /// <returns>
        ///   <c>true</c> if the specified member is to be considered; otherwise, <c>false</c>.
        /// </returns>
        protected override bool IsConsideredMember(MemberInfo member)
        {
            return member is PropertyInfo && base.IsConsideredMember(member);
        }
    }
}
