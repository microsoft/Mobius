using SerializationHelpers.Extensions.ExpressionNodes;
using Serialize.Linq.Factories;
using Serialize.Linq.Interfaces;
using Serialize.Linq.Nodes;
using Serialize.Linq.Serializers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SerializationHelpers.ComplexSerializers
{
    public class LinqExpressionSerializer : Serialize.Linq.Serializers.ExpressionSerializer
    {
        private readonly ISerializer _serializer;
        public LinqExpressionSerializer(ISerializer serializer, FactorySettings factorySettings = null) : base(serializer, factorySettings)
        {
            this._serializer = serializer;
        }

        protected override INodeFactory CreateFactory(Expression expression, FactorySettings factorySettings)
        {
            if (expression is LambdaExpression lambda)
                return new EnhancedDefaultNodeFactory(lambda.Parameters.Select(p => p.Type), factorySettings);
            return new Extensions.ExpressionNodes.NodeFactory(factorySettings);
        }              
    }
}
