using Serialize.Linq.Factories;
using Serialize.Linq.Nodes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Extensions.ExpressionNodes
{
    public class EnhancedTypeResolverNodeFactory : NodeFactory
    {
        private readonly Type[] _expectedTypes;
        public EnhancedTypeResolverNodeFactory(IEnumerable<Type> expectedTypes, FactorySettings factorySettings = null)
            : base(factorySettings)
        {
            if (expectedTypes == null)
                throw new ArgumentNullException(nameof(expectedTypes));
            _expectedTypes = expectedTypes.ToArray();
        }

        /// <summary>
        /// Determines whether the specified type is expected.
        /// </summary>
        /// <param name="declaredType">Type of the declared.</param>
        /// <returns>
        ///   <c>true</c> if type is expected; otherwise, <c>false</c>.
        /// </returns>
        private bool IsExpectedType(Type declaredType)
        {
            foreach (var expectedType in _expectedTypes)
            {
                if (declaredType == expectedType || declaredType.IsSubclassOf(expectedType))
                    return true;
                if (expectedType.GetTypeInfo().IsInterface)
                {
                    var resultTypes = declaredType.GetInterfaces();
                    if (resultTypes.Contains(expectedType))
                        return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Tries the get constant value from member expression.
        /// </summary>
        /// <param name="memberExpression">The member expression.</param>
        /// <param name="constantValue">The constant value.</param>
        /// <param name="constantValueType">Type of the constant value.</param>
        /// <returns></returns>
        /// <exception cref="System.NotSupportedException">MemberType ' + memberExpression.Member.MemberType + ' not yet supported.</exception>
        private bool TryGetConstantValueFromMemberExpression(
            MemberExpression memberExpression,
            out object constantValue,
            out Type constantValueType)
        {
            constantValue = null;
            constantValueType = null;

            FieldInfo parentField = null;
            while (true)
            {
                var run = memberExpression;
                while (true)
                {
                    if (!(run.Expression is MemberExpression next))
                        break;
                    run = next;
                }

                if (this.IsExpectedType(run.Member.DeclaringType))
                    return false;

                switch (memberExpression.Member)
                {
                    case FieldInfo field:
                        {
                            if (memberExpression.Expression != null)
                            {
                                if (memberExpression.Expression.NodeType == ExpressionType.Constant)
                                {
                                    var constantExpression = (ConstantExpression)memberExpression.Expression;
                                    var flags = this.GetBindingFlags();

                                    constantValue = constantExpression.Value;
                                    constantValueType = constantExpression.Type;
                                    var match = false;
                                    do
                                    {
                                        var fields = flags == null
                                            ? constantValueType.GetFields()
                                            : constantValueType.GetFields(flags.Value);
                                        var memberField = fields.Length > 1
                                            ? fields.SingleOrDefault(n => field.Name.Equals(n.Name))
                                            : fields.FirstOrDefault();
                                        if (memberField == null && parentField != null)
                                        {
                                            memberField = fields.Length > 1
                                                ? fields.SingleOrDefault(n => parentField.Name.Equals(n.Name))
                                                : fields.FirstOrDefault();
                                        }
                                        if (memberField == null)
                                            break;

                                        constantValueType = memberField.FieldType;
                                        constantValue = memberField.GetValue(constantValue);
                                        match = true;
                                    }
                                    while (constantValue != null && !KnownTypes.Match(constantValueType));

                                    return match;
                                }

                                if (memberExpression.Expression is MemberExpression subExpression)
                                {
                                    memberExpression = subExpression;
                                    parentField = parentField ?? field;
                                    continue;
                                }
                            }

                            if (field.IsPrivate || field.IsFamilyAndAssembly)
                            {
                                constantValue = field.GetValue(null);
                                return true;
                            }

                            break;
                        }
                    case PropertyInfo propertyInfo:
                        try
                        {
                            constantValue = Expression.Lambda(memberExpression).Compile().DynamicInvoke();

                            constantValueType = propertyInfo.PropertyType;
                            return true;
                        }
                        catch (InvalidOperationException)
                        {
                            constantValue = null;
                            return false;
                        }
                    default:
                        throw new NotSupportedException("MemberType '" + memberExpression.Member.GetType().Name + "' not yet supported.");
                }

                return false;
            }
        }

        /// <summary>
        /// Tries to inline an expression.
        /// </summary>
        /// <param name="memberExpression">The member expression.</param>
        /// <param name="inlineExpression">The inline expression.</param>
        /// <returns></returns>
        private bool TryToInlineExpression(MemberExpression memberExpression, out Expression inlineExpression)
        {
            inlineExpression = null;

            if (!(memberExpression.Member is FieldInfo))
                return false;

            if (memberExpression.Expression == null || memberExpression.Expression.NodeType != ExpressionType.Constant)
                return false;

            var constantExpression = (ConstantExpression)memberExpression.Expression;
            var flags = this.GetBindingFlags();
            var fields = flags == null
                ? constantExpression.Type.GetFields()
                : constantExpression.Type.GetFields(flags.Value);
            var memberField = fields.Single(n => memberExpression.Member.Name.Equals(n.Name));
            var constantValue = memberField.GetValue(constantExpression.Value);

            inlineExpression = constantValue as Expression;
            return inlineExpression != null;
        }

        /// <summary>
        /// Resolves the member expression.
        /// </summary>
        /// <param name="memberExpression">The member expression.</param>
        /// <returns></returns>
        private ExpressionNode ResolveMemberExpression(MemberExpression memberExpression)
        {
            if (this.TryToInlineExpression(memberExpression, out var inlineExpression))
                return this.Create(inlineExpression);

            return this.TryGetConstantValueFromMemberExpression(memberExpression, out var constantValue, out var constantValueType)
                ? new ConstantExpressionNode(this, constantValue, constantValueType)
                : base.Create(memberExpression);
        }

        /// <summary>
        /// Resolves the method call expression.
        /// </summary>
        /// <param name="methodCallExpression">The method call expression.</param>
        /// <returns></returns>
        private ExpressionNode ResolveMethodCallExpression(MethodCallExpression methodCallExpression)
        {
            if (methodCallExpression.Object is MemberExpression)
            {
                var member = methodCallExpression.Object as MemberExpression;
                object constantValue;
                Type constantValueType;

                if (this.TryGetConstantValueFromMemberExpression(member, out constantValue, out constantValueType))
                {
                    if (methodCallExpression.Arguments.Count == 0)
                        return new ConstantExpressionNode(this, Expression.Lambda(methodCallExpression).Compile().DynamicInvoke());
                }
            }
            else if (methodCallExpression.Object is ParameterExpression)
            {
                var member = methodCallExpression.Object as ParameterExpression;
                return new MethodCallExpressionNode(this, methodCallExpression);
            }

            else if (methodCallExpression.Method.Name == "ToString" && methodCallExpression.Method.ReturnType == typeof(string))
            {
                var constantValue = Expression.Lambda(methodCallExpression).Compile().DynamicInvoke();
                return new ConstantExpressionNode(this, constantValue);
            }
            return base.Create(methodCallExpression);
        }

        /// <summary>
        /// Creates the specified expression.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns></returns>
        public override ExpressionNode Create(Expression expression)
        {
            if (expression is MemberExpression member)
                return this.ResolveMemberExpression(member);

            if (expression is MethodCallExpression method)
                return this.ResolveMethodCallExpression(method);

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
    }
}
