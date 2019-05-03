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
    public class MethodCallExpressionNode : ExpressionNode<MethodCallExpression>
    {
        public MethodCallExpressionNode() { }

        public MethodCallExpressionNode(INodeFactory factory, MethodCallExpression expression)
            : base(factory, expression) { }

        #region DataMember
#if !SERIALIZE_LINQ_OPTIMIZE_SIZE
        [DataMember(EmitDefaultValue = false)]
#else
        [DataMember(EmitDefaultValue = false, Name = "A")]
#endif
        #endregion
        public ExpressionNodeList Arguments { get; set; }

        #region DataMember
#if !SERIALIZE_LINQ_OPTIMIZE_SIZE
        [DataMember(EmitDefaultValue = false)]
#else
        [DataMember(EmitDefaultValue = false, Name = "M")]
#endif
        #endregion
        public MethodInfoNode Method { get; set; }

        #region DataMember
#if !SERIALIZE_LINQ_OPTIMIZE_SIZE
        [DataMember(EmitDefaultValue = false)]
#else
        [DataMember(EmitDefaultValue = false, Name = "O")]
#endif
        #endregion
        public ExpressionNode Object { get; set; }

        protected override void Initialize(MethodCallExpression expression)
        {
            this.Arguments = new ExpressionNodeList(this.Factory, expression.Arguments);
            this.Method = new MethodInfoNode(this.Factory, expression.Method);
            this.Object = this.Factory.Create(expression.Object);
        }

        public override Expression ToExpression(IExpressionContext context)
        {
            Expression objectExpression = null;
            if (this.Object != null)
                objectExpression = this.Object.ToExpression(context);

            return Expression.Call(objectExpression, this.Method.ToMemberInfo(context), this.Arguments.Select(x => x.ToExpression(context)).ToArray());
        }
    }
}