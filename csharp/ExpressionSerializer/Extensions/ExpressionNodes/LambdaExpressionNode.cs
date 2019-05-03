using Serialize.Linq.Interfaces;
using Serialize.Linq.Nodes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using System.Text;

namespace SerializationHelpers.Extensions.ExpressionNodes
{
    public class LambdaExpressionNode : ExpressionNode<LambdaExpression>
    {
        public LambdaExpressionNode() { }

        public LambdaExpressionNode(INodeFactory factory, LambdaExpression expression)
            : base(factory, expression) { }

        #region DataMember
#if !SERIALIZE_LINQ_OPTIMIZE_SIZE
        [DataMember(EmitDefaultValue = false)]
#else
        [DataMember(EmitDefaultValue = false, Name = "B")]
#endif
        #endregion
        public ExpressionNode Body { get; set; }

        #region DataMember
#if !SERIALIZE_LINQ_OPTIMIZE_SIZE
        [DataMember(EmitDefaultValue = false)]
#else
        [DataMember(EmitDefaultValue = false, Name = "P")]
#endif
        #endregion
        public ExpressionNodeList Parameters { get; set; }

        protected override void Initialize(LambdaExpression expression)
        {
#if !WINDOWS_PHONE7
            this.Parameters = new ExpressionNodeList(this.Factory, expression.Parameters);
#else
            this.Parameters = new ExpressionNodeList(this.Factory, expression.Parameters.Select(p => (Expression)p));
#endif
            this.Body = this.Factory.Create(expression.Body);
        }

        public override Expression ToExpression(IExpressionContext context)
        {
            var body = this.Body.ToExpression(context);
            var parameters = this.Parameters.GetParameterExpressions(context).ToArray();

            var bodyParameters = GetNodes(body).OfType<ParameterExpression>().ToArray();
            for (var i = 0; i < parameters.Length; ++i)
            {
                var matchingParameter = bodyParameters.Where(p => p.Name == parameters[i].Name && p.Type == parameters[i].Type).ToArray();
                if (matchingParameter.Length == 1)
                    parameters[i] = matchingParameter.First();
            }

            return Expression.Lambda(this.Type.ToType(context), body, parameters);
        }

        internal IEnumerable<Expression> GetNodes(Expression expression)
        {
            foreach (var node in GetLinkNodes(expression))
            {
                foreach (var subNode in GetNodes(node))
                    yield return subNode;
            }
            yield return expression;
        }

        internal IEnumerable<Expression> GetLinkNodes(Expression expression)
        {
            switch (expression)
            {
                case LambdaExpression lambdaExpression:
                    {
                        yield return lambdaExpression.Body;
                        foreach (var parameter in lambdaExpression.Parameters)
                            yield return parameter;
                        break;
                    }
                case BinaryExpression binaryExpression:
                    yield return binaryExpression.Left;
                    yield return binaryExpression.Right;
                    break;
                case ConditionalExpression conditionalExpression:
                    yield return conditionalExpression.IfTrue;
                    yield return conditionalExpression.IfFalse;
                    yield return conditionalExpression.Test;
                    break;
                case InvocationExpression invocationExpression:
                    {
                        yield return invocationExpression.Expression;
                        foreach (var argument in invocationExpression.Arguments)
                            yield return argument;
                        break;
                    }
                case ListInitExpression listInitExpression:
                    yield return listInitExpression.NewExpression;
                    break;
                case MemberExpression memberExpression:
                    yield return memberExpression.Expression;
                    break;
                case MemberInitExpression memberInitExpression:
                    yield return memberInitExpression.NewExpression;
                    break;
                case MethodCallExpression methodCallExpression:
                    {
                        foreach (var argument in methodCallExpression.Arguments)
                            yield return argument;
                        if (methodCallExpression.Object != null)
                            yield return methodCallExpression.Object;
                        break;
                    }
                case NewArrayExpression newArrayExpression:
                    {
                        foreach (var item in newArrayExpression.Expressions)
                            yield return item;
                        break;
                    }
                case NewExpression newExpression:
                    {
                        foreach (var item in newExpression.Arguments)
                            yield return item;
                        break;
                    }
                case TypeBinaryExpression typeBinaryExpression:
                    yield return typeBinaryExpression.Expression;
                    break;
                case UnaryExpression unaryExpression:
                    yield return unaryExpression.Operand;
                    break;
            }
        }
    }
}
