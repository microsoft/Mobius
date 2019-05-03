using SerializationHelpers.Extensions.Enumerators;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Extensions.ExpressionNodes
{
    internal class ComplexPropertyMemberTypeFinder
    {
        /// <summary>
        /// Analyses the types.
        /// </summary>
        /// <param name="types">The types.</param>
        /// <param name="seen">The seen.</param>
        /// <param name="result">The result.</param>
        /// <returns></returns>
        private bool AnalyseTypes(IEnumerable<Type> types, ISet<Type> seen, ISet<Type> result)
        {
            return types != null
                && types.Aggregate(false, (current, type) => this.BuildTypes(type, seen, result) || current);
        }

        /// <summary>
        /// Analyses the type.
        /// </summary>
        /// <param name="baseType">Type of the base.</param>
        /// <param name="seen">The seen.</param>
        /// <param name="result">The result.</param>
        /// <returns></returns>
        private bool AnalyseType(Type baseType, ISet<Type> seen, ISet<Type> result)
        {
            bool retval;
            if (baseType.HasElementType)
            {
                if (!(retval = this.BuildTypes(baseType.GetElementType(), seen, result)))
                    retval = seen.Contains(baseType.GetElementType());
            }
            else
            {
                retval = true;
            }

            if (baseType.GetType().IsGenericType)
                retval = this.AnalyseTypes(baseType.GetGenericArguments(), seen, result) || retval;
            retval = this.AnalyseTypes(baseType.GetInterfaces(), seen, result) || retval;
            var subBaseType = baseType.GetType().BaseType;
            if (subBaseType != null && subBaseType != typeof(object))
                retval = this.BuildTypes(subBaseType, seen, result) || retval;
            return retval;
        }

        /// <summary>
        /// Builds the types.
        /// </summary>
        /// <param name="baseType">Type of the base.</param>
        /// <param name="seen">The seen.</param>
        /// <param name="result">The result.</param>
        /// <returns></returns>
        private bool BuildTypes(Type baseType, ISet<Type> seen, ISet<Type> result)
        {
            if (seen.Contains(baseType))
                return false;
            seen.Add(baseType);
            if (!this.AnalyseType(baseType, seen, result))
                return false;

            var enumerator = new ComplexPropertyMemberTypeEnumerator(baseType, BindingFlags.Instance | BindingFlags.Public);
            if (!enumerator.IsConsidered)
                return false;
            result.Add(baseType);

            var retval = false;
            while (enumerator.MoveNext())
            {
                var type = enumerator.Current;
                retval = this.BuildTypes(type, seen, result) || retval;
            }

            return retval;
        }

        /// <summary>
        /// Finds the types.
        /// </summary>
        /// <param name="baseType">Type of the base.</param>
        /// <returns></returns>
        public IEnumerable<Type> FindTypes(Type baseType)
        {
            var retval = new HashSet<Type>();
            this.BuildTypes(baseType, new HashSet<Type>(), retval);
            return retval;
        }
    }
}
