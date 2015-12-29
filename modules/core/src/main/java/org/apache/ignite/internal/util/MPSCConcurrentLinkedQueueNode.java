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
 * Node for {@link MPSCConcurrentLinkedQueue}.
 */
public class MPSCConcurrentLinkedQueueNode {
    /** Next field updater. */
    private static final AtomicReferenceFieldUpdater<MPSCConcurrentLinkedQueueNode, MPSCConcurrentLinkedQueueNode> NODE_UPD =
        AtomicReferenceFieldUpdater.newUpdater(MPSCConcurrentLinkedQueueNode.class, MPSCConcurrentLinkedQueueNode.class, "next");

    /** Value. */
    private Object val;

    /** Next node. */
    @SuppressWarnings("UnusedDeclaration")
    private volatile MPSCConcurrentLinkedQueueNode next;

    /**
     * Constructor.
     *
     * @param val Value.
     */
    MPSCConcurrentLinkedQueueNode(Object val)
    {
        this.val = val;
    }

    /**
     * Get value.
     *
     * @return Value.
     */
    Object value() {
        return val;
    }

    /**
     * Set value.
     *
     * @param val Value.
     */
    void value(Object val) {
        this.val = val;
    }

    /**
     * Get next node.
     *
     * @return Next node.
     */
    MPSCConcurrentLinkedQueueNode next() {
        return next;
    }

    /**
     * Set next node.
     *
     * @param next Next node.
     */
    void next(MPSCConcurrentLinkedQueueNode next)
    {
        NODE_UPD.lazySet(this, next);
    }
}
