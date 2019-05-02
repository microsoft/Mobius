using Serialize.Linq.Interfaces;
using Serialize.Linq.Nodes;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Extensions.ExpressionNodes
{
    public class ConstructorInfoNode : MemberNode<ConstructorInfo>
    {
        public ConstructorInfoNode() { }

        public ConstructorInfoNode(INodeFactory factory, ConstructorInfo memberInfo)
            : base(factory, memberInfo) { }

        /// <summary>
        /// Gets the member infos for the specified type.
        /// </summary>
        /// <param name="context">The expression context.</param>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        protected override IEnumerable<ConstructorInfo> GetMemberInfosForType(IExpressionContext context, Type type)
        {
            BindingFlags? flags = null;
            if (context != null)
                flags = context.GetBindingFlags();
            else if (this.Factory != null)
                flags = this.Factory.GetBindingFlags();
            return flags == null ? type.GetConstructors() : type.GetConstructors(flags.Value);
        }
    }
}
