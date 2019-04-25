using ExpressionSerializer.ComplexSerializer;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SerializationHelpers.Data
{
    public class LinqExpressionData
    {
        private String _expressionData;

        public LinqExpressionData(String expressionData)
        {
            this._expressionData = expressionData;
        }

        public Expression<TDelegate> ToExpression<TDelegate>() where TDelegate : Delegate
        {
            var serializer = new Serialize.Linq.Serializers.ExpressionSerializer(new JsonLinqSerializer());
            return (Expression<TDelegate>)serializer.DeserializeText(_expressionData);
        }

        public TDelegate ToFunc<TDelegate>() where TDelegate : Delegate
        {
            var serializer = new Serialize.Linq.Serializers.ExpressionSerializer(new JsonLinqSerializer());
            var expression = (Expression<TDelegate>)serializer.DeserializeText(_expressionData);
            return expression.Compile();
        }
    }
}
