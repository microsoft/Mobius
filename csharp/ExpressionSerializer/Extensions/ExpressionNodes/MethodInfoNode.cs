using Serialize.Linq.Interfaces;
using Serialize.Linq.Nodes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;

namespace SerializationHelpers.Extensions.ExpressionNodes
{
    public class MethodInfoNode : MemberNode<MethodInfo>
    {
        public MethodInfoNode() { }

        public MethodInfoNode(INodeFactory factory, MethodInfo memberInfo)
            : base(factory, memberInfo) { }

        protected override IEnumerable<MethodInfo> GetMemberInfosForType(IExpressionContext context, Type type)
        {
            BindingFlags? flags = null;
            if (context != null)
                flags = context.GetBindingFlags();
            else if (this.Factory != null)
                flags = this.Factory.GetBindingFlags();
            return flags == null ? type.GetMethods() : type.GetMethods(flags.Value);
        }

        #region DataMember
#if !SERIALIZE_LINQ_OPTIMIZE_SIZE
        [DataMember(EmitDefaultValue = false)]
#else
        [DataMember(EmitDefaultValue = false, Name = "I")]
#endif
        #endregion
        public bool IsGenericMethod { get; set; }

        #region DataMember
#if !SERIALIZE_LINQ_OPTIMIZE_SIZE
        [DataMember(EmitDefaultValue = false)]
#else
        [DataMember(EmitDefaultValue = false, Name = "G")]
#endif
        #endregion
        public TypeNode[] GenericArguments { get; set; }

        protected override void Initialize(MethodInfo memberInfo)
        {
            base.Initialize(memberInfo);
            if (!memberInfo.IsGenericMethod)
                return;

            this.IsGenericMethod = true;
            this.Signature = memberInfo.GetGenericMethodDefinition().ToString();
            this.GenericArguments = memberInfo.GetGenericArguments().Select(a => this.Factory.Create(a)).ToArray();
        }

        public override MethodInfo ToMemberInfo(IExpressionContext context)
        {
            var method = base.ToMemberInfo(context);
            if (method == null)
                return null;

            if (this.IsGenericMethod && this.GenericArguments != null && this.GenericArguments.Length > 0)
            {
                var arguments = this.GenericArguments
                    .Select(a => a.ToType(context))
                    .Where(t => t != null).ToArray();
                if (arguments.Length > 0)
                    method = method.MakeGenericMethod(arguments);
            }
            return method;
        }
    }
}
