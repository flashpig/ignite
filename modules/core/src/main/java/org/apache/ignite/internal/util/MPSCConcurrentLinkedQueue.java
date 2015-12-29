/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * MP-SC concurrent linked queue implementation based on Dmitry Vyukov's
 * <a href="http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue">
 *     Non-intrusive MPSC node-based queue</a>.
 */
public class MPSCConcurrentLinkedQueue<E>
{
    /** Tail field updater. */
    private static final AtomicReferenceFieldUpdater<MPSCConcurrentLinkedQueue, Node> TAIL_UPD =
        AtomicReferenceFieldUpdater.newUpdater(MPSCConcurrentLinkedQueue.class, Node.class, "tail");

    /** Head. */
    private Node head;

    /** Tail. */
    @SuppressWarnings({"UnusedDeclaration", "FieldCanBeLocal"})
    private volatile Node tail;

    /**
     * Constructor.
     */
    public MPSCConcurrentLinkedQueue()
    {
        head = new Node(null);

        tail = head;
    }

    /**
     * Offer element.
     *
     * @param e Element.
     */
    public void offer(final E e)
    {
        if (e == null)
            throw new IllegalArgumentException("Null are not allowed.");

        Node newTail = new Node(e);

        Node prevTail = TAIL_UPD.getAndSet(this, newTail);

        prevTail.setNext(newTail);
    }

    /**
     * Poll element.
     *
     * @return Element.
     */
    @SuppressWarnings("unchecked")
    public E poll()
    {
        final Node node = head.next;

        if (node != null)
        {
            Object val = node.val;

            node.val = null;

            head = node;

            return (E)val;
        }
        else
            return null;
    }

    /**
     * Node with data.
     */
    private static class Node
    {
        /** Next field updater. */
        public static final AtomicReferenceFieldUpdater<Node, Node> NODE_UPD =
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");

        /** Value. */
        private Object val;

        /** Next node. */
        @SuppressWarnings("UnusedDeclaration")
        private volatile Node next;

        /**
         * Constructor.
         *
         * @param val Value.
         */
        private Node(Object val)
        {
            this.val = val;
        }

        /**
         * Set next node.
         *
         * @param next Next node.
         */
        void setNext(Node next)
        {
            NODE_UPD.lazySet(this, next);
        }
    }
}