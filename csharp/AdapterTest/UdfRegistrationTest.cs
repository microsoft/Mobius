// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using Microsoft.Spark.CSharp.Proxy;
using Microsoft.Spark.CSharp.Sql;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    public class UdfRegistrationTest
    {
        [Test]
        public void TestRegisterFunction()
        {
            Mock<IUdfRegistrationProxy> mockUdfRegistrationProxy = new Mock<IUdfRegistrationProxy>();
            mockUdfRegistrationProxy.Setup(m => m.RegisterFunction(It.IsAny<string>(), It.IsAny<byte[]>(), It.IsAny<string>()));

            var udfRegistration = new UdfRegistration(mockUdfRegistrationProxy.Object);

            udfRegistration.RegisterFunction("Func0", () => "Func0");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func0", It.IsAny<byte[]>(), "string"));

            udfRegistration.RegisterFunction<string, string>("Func1", s => "Func1");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func1", It.IsAny<byte[]>(), "string"));

            udfRegistration.RegisterFunction<string, string, string>("Func2", (s1, s2) => "Func2");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func2", It.IsAny<byte[]>(), "string"));

            udfRegistration.RegisterFunction<string, string, string, string>("Func3", (s1, s2, s3) => "Func3");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func3", It.IsAny<byte[]>(), "string"));

            udfRegistration.RegisterFunction<string, string, string, string, string>("Func4", (s1, s2, s3, s4) => "Func4");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func4", It.IsAny<byte[]>(), "string"));

            udfRegistration.RegisterFunction<string, string, string, string, string, string>("Func5", (s1, s2, s3, s4, s5) => "Func5");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func5", It.IsAny<byte[]>(), "string"));

            udfRegistration.RegisterFunction<string, string, string, string, string, string, string>("Func6", (s1, s2, s3, s4, s5, s6) => "Func6");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func6", It.IsAny<byte[]>(), "string"));

            udfRegistration.RegisterFunction<string, string, string, string, string, string, string, string>("Func7", (s1, s2, s3, s4, s5, s6, s7) => "Func7");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func7", It.IsAny<byte[]>(), "string"));

            udfRegistration.RegisterFunction<string, string, string, string, string, string, string, string, string>("Func8", (s1, s2, s3, s4, s5, s6, s7, s8) => "Func8");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func8", It.IsAny<byte[]>(), "string"));

            udfRegistration.RegisterFunction<string, string, string, string, string, string, string, string, string, string>("Func9", (s1, s2, s3, s4, s5, s6, s7, s8, s9) => "Func9");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func9", It.IsAny<byte[]>(), "string"));

            udfRegistration.RegisterFunction<string, string, string, string, string, string, string, string, string, string, string>("Func10", (s1, s2, s3, s4, s5, s6, s7, s8, s9, s10) => "Func10");
            mockUdfRegistrationProxy.Verify(m => m.RegisterFunction("Func10", It.IsAny<byte[]>(), "string"));
        }
    }
}
