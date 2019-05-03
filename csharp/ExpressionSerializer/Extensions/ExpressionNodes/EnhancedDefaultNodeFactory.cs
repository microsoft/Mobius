using Serialize.Linq.Factories;
using Serialize.Linq.Interfaces;
using Serialize.Linq.Nodes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Extensions.ExpressionNodes
{
    public class EnhancedDefaultNodeFactory : INodeFactory
    {
        private readonly INodeFactory _innerFactory;
        private readonly Type[] _types;

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultNodeFactory"/> class.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="factorySettings">The factory settings to use.</param>
        public EnhancedDefaultNodeFactory(Type type, FactorySettings factorySettings = null)
            : this(new[] { type }, factorySettings) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="DefaultNodeFactory"/> class.
        /// </summary>
        /// <param name="types">The types.</param>
        /// <param name="factorySettings">The factory settings to use.</param>
        /// <exception cref="System.ArgumentNullException">types</exception>
        /// <exception cref="System.ArgumentException">types</exception>
        public EnhancedDefaultNodeFactory(IEnumerable<Type> types, FactorySettings factorySettings = null)
        {
            if (types == null)
                throw new ArgumentNullException(nameof(types));

            _types = types.ToArray();
            if (_types.Any(t => t == null))
                throw new ArgumentException("All types must be non-null.", nameof(types));
            Settings = factorySettings ?? new FactorySettings();
            _innerFactory = this.CreateFactory();
        }

        public FactorySettings Settings { get; }

        /// <summary>
        /// Creates the specified expression node an expression.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        public ExpressionNode Create(Expression expression)
        {
            return _innerFactory.Create(expression);
        }

        /// <summary>
        /// Creates the specified type node from a type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public TypeNode Create(Type type)
        {
            return _innerFactory.Create(type);
        }

        /// <summary>
        /// Gets binding flags to be used when accessing type members.
        /// </summary>
        public BindingFlags? GetBindingFlags()
        {
            return _innerFactory.GetBindingFlags();
        }

        /// <summary>
        /// Creates the factory.
        /// </summary>
        /// <returns></returns>
        private INodeFactory CreateFactory()
        {
            var expectedTypes = new HashSet<Type>();
            foreach (var type in _types)
                expectedTypes.UnionWith(GetComplexMemberTypes(type));
            return new EnhancedTypeResolverNodeFactory(expectedTypes, this.Settings);
        }

        /// <summary>
        /// Gets the complex member types.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        private static IEnumerable<Type> GetComplexMemberTypes(Type type)
        {
            var typeFinder = new ComplexPropertyMemberTypeFinder();
            return typeFinder.FindTypes(type);
        }
    }
}
