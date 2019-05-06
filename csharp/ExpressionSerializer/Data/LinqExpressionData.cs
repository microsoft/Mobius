using ExpressionSerializer.ComplexSerializer;
using SerializationHelpers.ComplexSerializers;
using SerializationHelpers.Context;
using Serialize.Linq;
using Serialize.Linq.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.Serialization;
using System.Text;

namespace SerializationHelpers.Data
{
    [Serializable]
    [DataContract]
    public class LinqExpressionData
    {
        [DataMember]
        internal String ExpressionData;

        public LinqExpressionData() { }

        public Expression<TDelegate> ToExpression<TDelegate>(IExpressionContext expressionContext = null) where TDelegate : Delegate
        {
            var serializer = new LinqExpressionSerializer(new JsonLinqSerializer());
            if (expressionContext == null)
            {
                expressionContext = new ExpressionContext { AllowPrivateFieldAccess = true };
            }
            return (Expression<TDelegate>)serializer.DeserializeText(ExpressionData, expressionContext);
        }

        public TDelegate ToFunc<TDelegate>(IExpressionContext expressionContext = null) where TDelegate : Delegate
        {
            var serializer = new LinqExpressionSerializer(new JsonLinqSerializer());
            if (expressionContext == null)
            {
                expressionContext = new ExpressionContext { AllowPrivateFieldAccess = true };
            }
            var expression = (Expression<TDelegate>)serializer.DeserializeText(ExpressionData, expressionContext);
            return expression?.Compile();
        }

        public bool Exists()
        {
            return !String.IsNullOrEmpty(this.ExpressionData);
        }
    }
}
