// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Reflection;

using Microsoft.Spark.CSharp.Interop.Ipc;
using Microsoft.Spark.CSharp.Services;

[assembly: InternalsVisibleTo("CSharpWorker")]
namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// A shared variable that can be accumulated, i.e., has a commutative and associative "add"
    /// operation. Worker tasks on a Spark cluster can add values to an Accumulator with the C{+=}
    /// operator, but only the driver program is allowed to access its value, using C{value}.
    /// Updates from the workers get propagated automatically to the driver program.
    /// 
    /// While C{SparkContext} supports accumulators for primitive data types like C{int} and
    /// C{float}, users can also define accumulators for custom types by providing a custom
    /// L{AccumulatorParam} object. Refer to the doctest of this module for an example.
    /// 
    /// See python implementation in accumulators.py, worker.py, PythonRDD.scala
    /// 
    /// </summary>
    [Serializable]
    public class Accumulator
    {
        internal static Dictionary<int, Accumulator> accumulatorRegistry = new Dictionary<int, Accumulator>();

        protected int accumulatorId;
        [NonSerialized]
        protected bool deserialized = true;
    }
    [Serializable]
    public class Accumulator<T> : Accumulator
    {
        [NonSerialized]
        internal T value;
        private readonly AccumulatorParam<T> accumulatorParam = new AccumulatorParam<T>();

        internal Accumulator(int accumulatorId, T value)
        {
            this.accumulatorId = accumulatorId;
            this.value = value;
            deserialized = false;
            accumulatorRegistry[accumulatorId] = this;
        }

        public T Value
        {
            // Get the accumulator's value; only usable in driver program
            get
            {
                if (deserialized)
                {
                    throw new ArgumentException("Accumulator.value cannot be accessed inside tasks");
                }
                return (Accumulator.accumulatorRegistry[accumulatorId] as Accumulator<T>).value;
            }
            // Sets the accumulator's value; only usable in driver program
            set
            {
                if (deserialized)
                {
                    throw new ArgumentException("Accumulator.value cannot be accessed inside tasks");
                }
                this.value = value;
            }
        }

        /// <summary>
        /// Adds a term to this accumulator's value
        /// </summary>
        /// <param name="term"></param>
        /// <returns></returns>
        public void Add(T term)
        {
            value = accumulatorParam.AddInPlace(value, term);
        }

        /// <summary>
        /// The += operator; adds a term to this accumulator's value
        /// </summary>
        /// <param name="self"></param>
        /// <param name="term"></param>
        /// <returns></returns>
        public static Accumulator<T> operator +(Accumulator<T> self, T term)
        {
            if (!accumulatorRegistry.ContainsKey(self.accumulatorId))
            {
                accumulatorRegistry[self.accumulatorId] = self;
            }
            self.Add(term);
            return self;
        }

        public override string ToString()
        {
            return string.Format("Accumulator<id={0}, value={1}>", accumulatorId, value);
        }
    }
    /// <summary>
    /// An AccumulatorParam that uses the + operators to add values. Designed for simple types
    /// such as integers, floats, and lists. Requires the zero value for the underlying type
    /// as a parameter.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    [Serializable]
    internal class AccumulatorParam<T>
    {
        /// <summary>
        /// Provide a "zero value" for the type
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        internal T Zero(T value)
        {
            return default(T);
        }
        /// <summary>
        /// Add two values of the accumulator's data type, returning a new value;
        /// </summary>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <returns></returns>
        internal T AddInPlace(T value1, T value2)
        {
            dynamic d1 = value1, d2 = value2;
            d1 += d2;
            return d1;
        }
    }

    /// <summary>
    /// A simple TCP server that intercepts shutdown() in order to interrupt
    /// our continuous polling on the handler.
    /// </summary>
    internal class AccumulatorServer : System.Net.Sockets.TcpListener
    {
        private readonly ILoggerService logger = LoggerServiceFactory.GetLogger(typeof(AccumulatorServer));
        private bool serverShutdown;

        internal AccumulatorServer()
            : base(IPAddress.Loopback, 0)
        {

        }

        internal void Shutdown()
        {
            serverShutdown = true;
            base.Stop();
        }

        internal int StartUpdateServer()
        {
            base.Start();
            Task.Run(() =>
            {
                try
                {
                    IFormatter formatter = new BinaryFormatter();
                    using (Socket s = AcceptSocket())
                    using (var ns = new NetworkStream(s))
                    {
                        while (!serverShutdown)
                        {
                            int numUpdates = SerDe.ReadInt(ns);
                            for (int i = 0; i < numUpdates; i++)
                            {
                                var ms = new MemoryStream(SerDe.ReadBytes(ns));
                                KeyValuePair<int, dynamic> update = (KeyValuePair<int, dynamic>)formatter.Deserialize(ms);
                                Accumulator accumulator = Accumulator.accumulatorRegistry[update.Key];
                                accumulator.GetType().GetMethod("Add").Invoke(accumulator, new object[] { update.Value });
                            }
                            ns.WriteByte((byte)1);  // acknowledge byte other than -1
                            ns.Flush();
                        }
                    }
                }
                catch (SocketException e)
                {
                    if (e.ErrorCode != 10004)   // A blocking operation was interrupted by a call to WSACancelBlockingCall - TcpListener.Stop cancelled AccepSocket as expected
                        throw e;
                }
                catch (Exception e)
                {
                    logger.LogError(e.ToString());
                    throw;
                }
            });
            
            return (base.LocalEndpoint as IPEndPoint).Port;
        }
    }
}
