// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using Microsoft.Spark.CSharp.Core;
using Microsoft.Spark.CSharp.Proxy;
using Moq;
using NUnit.Framework;

namespace AdapterTest
{
    [TestFixture]
    class HadoopConfigurationTest
    {
        private string name, value;

        [Test]
        public void TestGetterSetter()
        {
            Mock<IHadoopConfigurationProxy> hadoopConfProxy = new Mock<IHadoopConfigurationProxy>();

            hadoopConfProxy.Setup(m => m.Get(It.IsAny<string>(), It.IsAny<string>())).Returns("valueofproperty");

            hadoopConfProxy.Setup(m => m.Set(It.IsAny<string>(), It.IsAny<string>()))
                .Callback<string, string>(ValueSetter);

            var hadoopConf = new HadoopConfiguration(hadoopConfProxy.Object);

            var returnValue = hadoopConf.Get("somename", "somedefaultvalue");
            Assert.AreEqual("valueofproperty", returnValue);

            hadoopConf.Set("propertyname", "propertyvalue");

            Assert.AreEqual("propertyname", name);
            Assert.AreEqual("propertyvalue", value);
        }

        private void ValueSetter(string name, string value)
        {
            this.name = name;
            this.value = value;
        }
    }
}
