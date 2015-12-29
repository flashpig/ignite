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

/**
 * MP-SC concurrent linked queue implementation based on Dmitry Vyukov's
 * <a href="http://www.1024cores.net/home/lock-free-algorithms/queues/non-intrusive-mpsc-node-based-queue">
 *     Non-intrusive MPSC node-based queue</a>.
 */

public class MPSCConcurrentLinkedQueue<E> extends MPSCConcurrentLinkedQueuePadding
{
    /** Head. */
    private MPSCConcurrentLinkedQueueNode head;

    /**
     * Constructor.
     */
    public MPSCConcurrentLinkedQueue()
    {
        head = new MPSCConcurrentLinkedQueueNode(null);

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

        MPSCConcurrentLinkedQueueNode newTail = new MPSCConcurrentLinkedQueueNode(e);

        MPSCConcurrentLinkedQueueNode prevTail = TAIL_UPD.getAndSet(this, newTail);

        prevTail.next(newTail);
    }

    /**
     * Poll element.
     *
     * @return Element.
     */
    @SuppressWarnings("unchecked")
    public E poll()
    {
        final MPSCConcurrentLinkedQueueNode node = head.next();

        if (node != null)
        {
            Object val = node.value();

            node.value(null);

            head = node;

            return (E)val;
        }
        else
            return null;
    }
}