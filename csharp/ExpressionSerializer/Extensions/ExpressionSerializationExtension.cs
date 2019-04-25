using ExpressionSerializer.ComplexSerializer;
using SerializationHelpers.Data;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;

namespace SerializationHelpers.Extensions
{
    public static class ExpressionSerializationExtension
    {
        public static LinqExpressionData ToExpressionData(this Expression expression)
        {
            var serializer = new Serialize.Linq.Serializers.ExpressionSerializer(new JsonLinqSerializer());
            var expressionValue = serializer.SerializeText(expression);
            return new LinqExpressionData(expressionValue);
        }
    }
}
