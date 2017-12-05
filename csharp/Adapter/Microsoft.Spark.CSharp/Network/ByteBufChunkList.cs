// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.

using System;
using System.Diagnostics;
using System.Text;

namespace Microsoft.Spark.CSharp.Network
{
    /// <summary>
    /// ByteBufChunkList class represents a simple linked like list used to store ByteBufChunk objects
    /// based on its usage.
    /// </summary>
    internal class ByteBufChunkList
    {
        private readonly int maxUsage;
        private readonly int minUsage;

        internal readonly ByteBufChunkList NextList;
        internal ByteBufChunk head;

        /// <summary>
        /// The previous ByteBufChunkList. This is only update once when create the linked like list
        /// of ByteBufChunkList in ByteBufPool constructor.
        /// </summary>
        public ByteBufChunkList PrevList;

        /// <summary>
        /// Initializes a ByteBufChunkList instance with the next ByteBufChunkList and minUsage and maxUsage
        /// </summary>
        /// <param name="nextList">The next item of this ByteBufChunkList</param>
        /// <param name="minUsage">The definition of minimum usage to contain ByteBufChunk</param>
        /// <param name="maxUsage">The definition of maximum usage to contain ByteBufChunk</param>
        public ByteBufChunkList(ByteBufChunkList nextList, int minUsage, int maxUsage)
        {
            NextList = nextList;
            this.minUsage = minUsage;
            this.maxUsage = maxUsage;
        }

        /// <summary>
        /// Add the ByteBufChunk to this ByteBufChunkList linked-list based on ByteBufChunk's usage.
        /// So it will be moved to the right ByteBufChunkList that has the correct minUsage/maxUsage.
        /// </summary>
        /// <param name="chunk">The ByteBufChunk to be added</param>
        public void Add(ByteBufChunk chunk)
        {
            if (chunk.Usage >= maxUsage)
            {
                NextList.Add(chunk);
                return;
            }

            AddInternal(chunk);
        }

        /// <summary>
        /// Allocates a ByteBuf from this ByteBufChunkList if it is not empty.
        /// </summary>
        /// <param name="byteBuf">The allocated ByteBuf</param>
        /// <returns>true, if the ByteBuf be allocated; otherwise, false.</returns>
        public bool Allocate(out ByteBuf byteBuf)
        {
            if (head == null)
            {
                // This ByteBufChunkList is empty
                byteBuf = default(ByteBuf);
                return false;
            }

            for (var cur = head; ;)
            {
                if (!cur.Allocate(out byteBuf))
                {
                    cur = cur.Next;
                    if (cur == null)
                    {
                        return false;
                    }
                }
                else
                {
                    if (cur.Usage < maxUsage) return true;

                    Remove(cur);
                    NextList.Add(cur);
                    return true;
                }
            }
        }

        /// <summary>
        /// Releases the segment back to its ByteBufChunk.
        /// </summary>
        /// <param name="chunk">The ByteBufChunk that contains the ByteBuf</param>
        /// <param name="byteBuf">The ByteBuf to be released.</param>
        /// <returns>
        /// true, if the ByteBuf be released and NOT need to destroy the
        /// ByteBufChunk (its usage is 0); otherwise, false.
        /// </returns>
        public bool Free(ByteBufChunk chunk, ByteBuf byteBuf)
        {
            chunk.Free(byteBuf);
            if (chunk.Usage >= minUsage) return true;

            Remove(chunk);
            // Move the ByteBufChunk down the ByteBufChunkList linked-list.
            return MoveInternal(chunk);
        }

        private bool Move(ByteBufChunk chunk)
        {
            if (chunk.Usage < minUsage)
            {
                // Move the ByteBufChunk down the ByteBufChunkList linked-list
                return MoveInternal(chunk);
            }

            // ByteBufChunk fits into this ByteBufChunkList, adding it here.
            AddInternal(chunk);
            return true;
        }

        /// <summary>
        /// Adds the ByteBufChunk to this ByteBufChunkList
        /// </summary>
        private void AddInternal(ByteBufChunk chunk)
        {
            chunk.Parent = this;
            if (head == null)
            {
                head = chunk;
                chunk.Prev = null;
                chunk.Next = null;
            }
            else
            {
                chunk.Prev = null;
                chunk.Next = head;
                head.Prev = chunk;
                head = chunk;
            }
        }

        /// <summary>
        /// Moves the ByteBufChunk down the ByteBufChunkList linked-list so it will end up in the right
        /// ByteBufChunkList that has the correct minUsage/maxUsage in respect to ByteBufChunk.Usage.
        /// </summary>
        private bool MoveInternal(ByteBufChunk chunk)
        {
            if (PrevList == null)
            {
                // If there is no previous ByteBufChunkList so return false which result in
                // having the ByteBufChunk destroyed and memory associated with the ByteBufChunk
                // will be released.
                Debug.Assert(chunk.Usage == 0);
                return false;
            }
            
            return PrevList.Move(chunk);
        }

        /// <summary>
        /// Remove the ByteBufChunk from this ByteBufChunkList
        /// </summary>
        private void Remove(ByteBufChunk chunk)
        {
            Debug.Assert(chunk != null);
            if (chunk == head)
            {
                head = chunk.Next;
                if (head != null)
                {
                    head.Prev = null;
                }
            }
            else
            {
                var next = chunk.Next;
                chunk.Prev.Next = next;
                if (next != null)
                {
                    next.Prev = chunk.Prev;
                }
            }
        }

        /// <summary>
        /// Returns a readable string for this ByteBufChunkList
        /// </summary>
        public override string ToString()
        {
            if (head == null)
            {
                return "none";
            }

            var buf = new StringBuilder();
            for (var cur = head; ;)
            {
                buf.Append(cur);
                cur = cur.Next;
                if (cur == null)
                {
                    break;
                }
                buf.Append(Environment.NewLine);
            }

            return buf.ToString();
        }
    }
}