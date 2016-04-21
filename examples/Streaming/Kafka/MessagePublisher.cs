// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Threading;

/*
using Kafka.Client.Cfg;
using Kafka.Client.Messages;
using Kafka.Client.Producers;
*/

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// Publishes Messages that can be used with SparkClrKafka example.
    /// Messages published are string values in the format: [timestamp],[loglevel],[logmessage]
    /// </summary>
    class MessagePublisher
    {
        /***************************************************************************************************************
        * ********* To publish samples messages to Kafka, uncomment the commented out lines in this file and ********* *
        * ********* add reference to ZookeeperNet.dll (https://www.nuget.org/packages/ZooKeeper.Net/) and ************ *
        * ********* KafkaNet.dll (https://github.com/Microsoft/CSharpClient-for-Kafka)  ****************************** *
        ****************************************************************************************************************/

        private static readonly string[] LogLevels = { "Info", "Debug", "Error", "Fatal" };
        private static readonly Random RandomLogLevel = new Random();
        private static long batchIndex = 1;

        private static void SendMessageBatch(object state)
        {
/*
            var kafkaProducerConfig = new ProducerConfiguration(new List<BrokerConfiguration>
            {
                new BrokerConfiguration {Host = "localhost", Port = 9092}
            });
            var kafkaProducer = new Producer(kafkaProducerConfig);
*/
            var topicName = "<kafkaTopicName>";

            var now = DateTime.Now.Ticks;
            for (int messageIndex = 1; messageIndex <= 5; messageIndex++)
            {
                try
                {
                    var message = string.Format("{0},{1},{2}", now, LogLevels[RandomLogLevel.Next(4)], "LogMessage id " + Guid.NewGuid());
                    //kafkaProducer.Send(new ProducerData<string, Message>(topicName, new Message(Encoding.UTF8.GetBytes(message))));
                }
                catch (Exception exception)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("{0} > Exception in message {3} in batch {2}: {1}", DateTime.Now, exception.Message, batchIndex, messageIndex);
                    Console.ResetColor();
                }
            }

            Console.WriteLine("Completed sending all messages in batch {0}", batchIndex++);
        }

        public static void Publish()
        {
            var timer = new Timer(SendMessageBatch, null, 0, 60000);
            Thread.Sleep(Timeout.Infinite);
        }
       
/*      
        public static void Main(string[] args)
        {
            Publish();
        }
 */
        
    }
}
