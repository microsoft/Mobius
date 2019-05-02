using Serialize.Linq.Factories;
using Serialize.Linq.Interfaces;
using Serialize.Linq.Nodes;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Extensions.ExpressionNodes
{
    public class NodeFactory : INodeFactory
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NodeFactory"/> class.
        /// </summary>
        public NodeFactory()
            : this(null) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="NodeFactory"/> class.
        /// </summary>
        /// <param name="factorySettings">The factory settings to use.</param>
        public NodeFactory(FactorySettings factorySettings)
        {
            Settings = factorySettings ?? new FactorySettings();
        }

        public FactorySettings Settings { get; }

        /// <summary>
        /// Creates an expression node from an expression.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentException">Unknown expression of type  + expression.GetType()</exception>
        public virtual ExpressionNode Create(Expression expression)
        {
            if (expression == null)
                return null;

            if (expression is BinaryExpression binaryExpression) return new BinaryExpressionNode(this, binaryExpression);
            if (expression is ConditionalExpression conditionalExpression) return new ConditionalExpressionNode(this, conditionalExpression);
            if (expression is ConstantExpression constantExpression) return new ConstantExpressionNode(this, constantExpression);
            if (expression is InvocationExpression invocationExpression) return new InvocationExpressionNode(this, invocationExpression);
            if (expression is LambdaExpression lambdaExpression) return new LambdaExpressionNode(this, lambdaExpression);
            if (expression is ListInitExpression listInitExpression) return new ListInitExpressionNode(this, listInitExpression);
            if (expression is MemberExpression memberExpression) return new MemberExpressionNode(this, memberExpression);
            if (expression is MemberInitExpression memberInitExpression) return new MemberInitExpressionNode(this, memberInitExpression);
            if (expression is MethodCallExpression methodCallExpression) return new MethodCallExpressionNode(this, methodCallExpression);
            if (expression is NewArrayExpression newArrayExpression) return new NewArrayExpressionNode(this, newArrayExpression);
            if (expression is NewExpression newExpression) return new NewExpressionNode(this, newExpression);
            if (expression is ParameterExpression parameterExpression) return new ParameterExpressionNode(this, parameterExpression);
            if (expression is TypeBinaryExpression typeBinaryExpression) return new TypeBinaryExpressionNode(this, typeBinaryExpression);
            if (expression is UnaryExpression unaryExpression) return new UnaryExpressionNode(this, unaryExpression);

            throw new ArgumentException("Unknown expression of type " + expression.GetType());
        }

        /// <summary>
        /// Creates an type node from a type.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        public TypeNode Create(Type type)
        {
            return new TypeNode(this, type);
        }

        /// <summary>
        /// Gets binding flags to be used when accessing type members.
        /// </summary>
        public BindingFlags? GetBindingFlags()
        {
            if (!this.Settings.AllowPrivateFieldAccess)
                return null;

            return BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;
        }
    }
}