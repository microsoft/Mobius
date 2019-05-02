using ExpressionSerializer.ComplexSerializer;
using SerializationHelpers.ComplexSerializers;
using SerializationHelpers.Data;
using Serialize.Linq.Factories;
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
            var factory = new FactorySettings { AllowPrivateFieldAccess = true };
            var serializer = new LinqExpressionSerializer(new JsonLinqSerializer(), factory);
            var expressionValue = serializer.SerializeText(expression);
            return new LinqExpressionData { ExpressionData = expressionValue };
        }
    }
}
