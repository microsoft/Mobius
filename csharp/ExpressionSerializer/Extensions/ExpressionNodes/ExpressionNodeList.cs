using Serialize.Linq.Interfaces;
using Serialize.Linq.Nodes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SerializationHelpers.Extensions.ExpressionNodes
{
    public class ExpressionNodeList : List<ExpressionNode>
    {
        public ExpressionNodeList() { }

        public ExpressionNodeList(INodeFactory factory, IEnumerable<Expression> items)
        {
            if (factory == null)
                throw new ArgumentNullException("factory");
            if (items == null)
                throw new ArgumentNullException("items");
            this.AddRange(items.Select(factory.Create));
        }

        internal IEnumerable<Expression> GetExpressions(IExpressionContext context)
        {
            return this.Select(e => e.ToExpression(context));
        }

        internal IEnumerable<ParameterExpression> GetParameterExpressions(IExpressionContext context)
        {
            return this.OfType<ParameterExpressionNode>().Select(e => (ParameterExpression)e.ToExpression(context));
        }
    }
}
