using Serialize.Linq;
using Serialize.Linq.Nodes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace SerializationHelpers.Context
{
    public class WorkerExpressionContext : ExpressionContext
    {
        private readonly ConcurrentDictionary<string, Type> _typeCache = new ConcurrentDictionary<string, Type>();
        public WorkerExpressionContext() : base(new WorkerAssemblyLoader())
        {
            this.AllowPrivateFieldAccess = true;
        }

        public override Type ResolveType(TypeNode node)
        {
            if (node == null)
                throw new ArgumentNullException(nameof(node));

            if (string.IsNullOrWhiteSpace(node.Name))
                return null;

            return _typeCache.GetOrAdd(node.Name, n =>
            {
                var type = Type.GetType(n);
                if (type == null)
                {
                    var assemblies = this.GetAssemblies();
                    foreach (var assembly in assemblies)
                    {
                        type = assembly.GetType(n);
                        if (type != null)
                            break;
                        else
                        {
                            type = GetLoadableTypes(assembly).Where(x => x.AssemblyQualifiedName == n).FirstOrDefault();
                            if (type != null) break;
                        }
                            
                    }
                }

                return type;
            });
        }

        private IEnumerable<Type> GetLoadableTypes(Assembly assembly)
        {
            if (assembly == null) throw new ArgumentNullException(nameof(assembly));
            try
            {
                return assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException e)
            {
                return e.Types.Where(t => t != null);
            }
        }
    }
}
