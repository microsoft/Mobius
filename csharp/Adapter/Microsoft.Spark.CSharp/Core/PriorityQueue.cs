// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Collections;
using System.Collections.Generic;

namespace Microsoft.Spark.CSharp.Core
{
    /// <summary>
    /// A bounded priority queue implemented with max binary heap.
    /// 
    /// Construction steps:
    ///  1. Build a Max Heap of the first k elements.
    ///  2. For each element after the kth element, compare it with the root of the max heap,
    ///    a. If the element is less than the root, replace root with this element, heapify.
    ///    b. Else ignore it.
    /// </summary>
    [Serializable]
    internal class PriorityQueue<T> : IEnumerable<T> where T : IComparable<T>
    {
        // The number of elements in the priority queue.
        private int elementCount;
        private T[] queue;
        private Comparer<T> comparer;

        /// <summary>
        /// Constructor of PriorityQueue type.
        /// </summary>
        internal PriorityQueue(int queueSize, Comparer<T> comparer)
        {
            this.comparer = comparer;
            queue = new T[queueSize];
        }

        /// <summary>
        /// Inserts the specified element into this priority queue.
        /// </summary>
        internal void Offer(T e)
        {
            if (ReferenceEquals(null, e))
            {
                throw new NullReferenceException();
            }

            var i = elementCount;
            if (i >= queue.Length)
            {
                if (GT(queue[0], e)) // compare it with root of the heap
                {
                    queue[0] = e;
                    SiftDownHeapRoot();
                }

                return;
            }

            elementCount = i + 1;
            if (i == 0)
            {
                queue[0] = e;
            }
            else
            {
                SiftUp(i, e);
            }
        }

        private void SiftDownHeapRoot()
        {
            var x = queue[0];
            var half = (int)((uint)elementCount >> 1);
            var k = 0;

            while (k < half)
            {
                var child = (k << 1) + 1;
                var c = queue[child];
                var right = child + 1;
                if (right < elementCount && GT(queue[right], c))
                {
                    c = queue[child = right];
                }

                if (GE(x, c))
                {
                    break;
                }

                queue[k] = c;
                k = child;
            }

            queue[k] = x;
        }

        private void SiftUp(int k, T x)
        {
            while (k > 0)
            {
                var parent = (int)((uint)(k - 1) >> 1);
                var e = queue[parent];
                if (GE(e, x)) // if parent >= child, stop
                {
                    break;
                }

                queue[k] = e;
                k = parent;
            }

            queue[k] = x;
        }

        // helper method for comparision
        private bool GT(T a, T b)
        {
            return comparer.Compare(a, b) > 0;
        }

        // great or equal, helper method for comparision
        private bool GE(T a, T b)
        {
            return comparer.Compare(a, b) >= 0;
        }

        public IEnumerator<T> GetEnumerator()
        {
            for (var i = 0; i < elementCount; i++)
            {
                yield return queue[i];
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
