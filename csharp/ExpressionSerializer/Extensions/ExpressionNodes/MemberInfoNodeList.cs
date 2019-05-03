using Serialize.Linq.Interfaces;
using Serialize.Linq.Nodes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Extensions.ExpressionNodes
{
    public class MemberInfoNodeList : List<MemberInfoNode>
    {
        public MemberInfoNodeList() { }

        public MemberInfoNodeList(INodeFactory factory, IEnumerable<MemberInfo> items = null)
        {
            if (factory == null)
                throw new ArgumentNullException("factory");
            if (items != null)
                this.AddRange(items.Select(m => new MemberInfoNode(factory, m)));
        }

        public IEnumerable<MemberInfo> GetMembers(IExpressionContext context)
        {
            return this.Select(m => m.ToMemberInfo(context));
        }
    }
}
