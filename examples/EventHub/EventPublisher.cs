// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Threading;

namespace Microsoft.Spark.CSharp.Examples
{
    /// <summary>
    /// Publishes Events that can be used with SparkCLREventHub example.
    /// Events published are string values in the format: [timestamp],[loglevel],[logmessage]
    /// </summary>
    class EventPublisher
    {
        /******************************************************
         * Following connection parameters to EventHub need to 
         * be set appropriately before publishing
         *****************************************************/
        private static readonly string EventHubName = "<name>";
        private static readonly string Namespace = "<namespace>";
        private static readonly string KeyName = "<keyname>";
        private static readonly string Key = "<key>";
        private static readonly string ConnectionStringFormat = "Endpoint=sb://{0}.servicebus.windows.net/;SharedAccessKeyName={1};SharedAccessKey={2}";

        private static readonly string[] LogLevels = { "Info", "Debug", "Error", "Fatal" };
        private static readonly Random RandomLogLevel = new Random();
        private static long batchIndex = 1;

        /***************************************************************************************************************************************************
        * ********* To publish samples events to EventHubs, uncomment package references for EventHub in packages.config uncomment the following line **** *
        ***************************************************************************************************************************************************/
        //private static readonly EventHubClient EventHubClient = EventHubClient.CreateFromConnectionString(string.Format(ConnectionStringFormat, Namespace, KeyName, Key), EventHubName);

        private static void SendMessageBatch(object state)
        {
            var now = DateTime.Now.Ticks;
            for (int messageIndex = 1; messageIndex <= 100; messageIndex++)
            {
                try
                {
                    var message = string.Format("{0},{1},{2}", now, LogLevels[RandomLogLevel.Next(4)], "LogMessage id " + Guid.NewGuid());
                    /***************************************************************************************************************************************************
                    * ********* To publish samples events to EventHubs, uncomment package references for EventHub in packages.config uncomment the following line **** *
                    ***************************************************************************************************************************************************/
                    //EventHubClient.Send(new EventData(Encoding.UTF8.GetBytes(message)));
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

        /***************************************************************************************************************************************************
        * ********* To publish samples events to EventHubs, uncomment package references for EventHub in packages.config uncomment the following line **** *
        ***************************************************************************************************************************************************/
        /*
        public static void Main(string[] args)
        {
            Publish();
        }
        */
    }
}
