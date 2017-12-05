// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Spark.CSharp.Configuration;
using Microsoft.Spark.CSharp.Network;

namespace AdapterTest.Mocks
{
    //Fakes is not supported in AppVeyor (CI that will probably be used with CSharpSpark in GitHub)
    //Using custom implementation of mock for now. Replace with Moq or similiar framework //TODO
    internal class MockConfigurationService : IConfigurationService
    {
        internal void InjectCSharpSparkWorkerPath(string path)
        {
            workerPath = path;
        }

        private string workerPath;
        public string GetCSharpWorkerExePath()
        {
            return workerPath;
        }

        public SocketWrapperType GetCSharpSocketType()
        {
            return SocketWrapperType.Normal;
        }

        public int BackendPortNumber
        {
            get { throw new NotImplementedException(); }
        }

        public bool IsShipCSharpSparkBinariesToExecutors
        {
            get { throw new NotImplementedException(); }
        }

        public IEnumerable<string> GetDriverFiles()
        {
            return new string[] {};
        }
    }
}
