// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Spark.CSharp.Interop.Ipc
{
    /// <summary>
    /// Defines the behavior of socket implementation in SparkCLR
    /// </summary>
    public interface ISparkCLRSocket : IDisposable
    {
        void Initialize(int portNumber);
        void Write(byte[] value);
        void Write(int value);
        void Write(long value);
        void Write(string value);
        byte[] ReadBytes(int length);
        char ReadChar();
        int ReadInt();
        long ReadLong();
        string ReadString();
        string ReadString(int length);
        double ReadDouble();
        bool ReadBoolean();
        IDisposable InitializeStream();
        void Flush();

    }
}
