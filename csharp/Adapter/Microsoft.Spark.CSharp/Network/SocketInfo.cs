using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Interop.Ipc;

namespace Microsoft.Spark.CSharp.Network
{
	public class SocketInfo
	{
		public readonly int Port;
		public readonly string Secret;

		public SocketInfo(int port, string secret)
		{
			Port = port;
			Secret = secret;
		}

		public static SocketInfo Parse(object o)
		{
			var oo = o as List<JvmObjectReference>;
			if (oo == null) throw new Exception(o.ToString() + " is not socket info "+typeof(List<JvmObjectReference>)+" "+o.GetType());
			return new SocketInfo(int.Parse(oo[0].ObjectToString()), oo[1].ObjectToString());
		}
	}
}
