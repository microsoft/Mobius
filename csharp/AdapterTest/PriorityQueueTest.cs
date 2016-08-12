// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Spark.CSharp.Core;
using NUnit.Framework;

namespace AdapterTest
{
    /// <summary>
    /// Validates functionality of PriorityQueue
    /// </summary>
    [TestFixture]
    class PriorityQueueTest
    {
        internal List<int> GetRandomList(int size)
        {
            var rand = new Random(DateTime.Now.Millisecond);
            return Enumerable.Range(0, size).Select(i => rand.Next()).ToList();
        }

        [Test]
        public void Test()
        { 
            // build a random list
            var randoms = GetRandomList(60);
            
            // create a priority queue
            var size = 6;
            var queue = new PriorityQueue<int>(size, Comparer<int>.Create((x, y) => x - y));

            // feed numbers to queue
            randoms.ForEach(queue.Offer);
            randoms.Sort();

            // verify
            var expected = randoms.Take(size);
            Assert.AreEqual(expected.ToArray(), queue.OrderBy(x => x).ToArray());
        }

        [Test]
        public void TestDuplication()
        {
            var numbers = new [] { -1, -1, -1, 1, 1, 2, 3, 9, 20, 15, 11 }.ToList();

            var queue = new PriorityQueue<int>(1, Comparer<int>.Create((x, y) => x - y));
            numbers.ForEach(queue.Offer);
            Assert.AreEqual(-1, queue.ToArray()[0]);

            queue = new PriorityQueue<int>(3, Comparer<int>.Create((x, y) => x - y));
            numbers.ForEach(queue.Offer);
            queue.ToList().ForEach(x => Assert.AreEqual(-1, x));

            queue = new PriorityQueue<int>(4, Comparer<int>.Create((x, y) => x - y));
            numbers.ForEach(queue.Offer);
            Assert.AreEqual(1, queue.OrderBy(x => x).Last());

            queue = new PriorityQueue<int>(100, Comparer<int>.Create((x, y) => x - y));
            numbers.ForEach(queue.Offer);
            Assert.AreEqual(20, queue.OrderBy(x => x).Last());
        }

        [Test]
        public void TestReverseOrder()
        {
            var randoms = GetRandomList(100);
            var queue = new PriorityQueue<int>(3, Comparer<int>.Create((x, y) => y - x));
            randoms.ForEach(queue.Offer);
            var expected = randoms.OrderByDescending(x => x).Take(3);
            Assert.AreEqual(expected.ToArray(), queue.OrderByDescending(x => x).ToArray());
        }

        [Test]
        public void TestNull()
        {
            Assert.Throws<NullReferenceException>(() => new PriorityQueue<string>(5, Comparer<string>.Create((x, y) => String.Compare(x, y, StringComparison.Ordinal))).Offer(null));
        }
    }
}
