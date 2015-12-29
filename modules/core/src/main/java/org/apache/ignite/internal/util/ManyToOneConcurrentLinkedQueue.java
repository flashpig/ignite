package org.apache.ignite.internal.util;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 *
 */
public class ManyToOneConcurrentLinkedQueue<E>
{
    protected static final AtomicReferenceFieldUpdater<ManyToOneConcurrentLinkedQueue, Node> TAIL_UPD =
        AtomicReferenceFieldUpdater.newUpdater(ManyToOneConcurrentLinkedQueue.class, Node.class, "tail");

    protected volatile ManyToOneConcurrentLinkedQueue.Node tail;

    private Node head;

    public ManyToOneConcurrentLinkedQueue()
    {
        head = new Node(null);
        TAIL_UPD.lazySet(this, head);
    }

    public boolean offer(final E e)
    {
        if (null == e)
        {
            throw new NullPointerException("element cannot be null");
        }

        final Node newTail = new Node(e);
        final Node prevTail = swapTail(newTail);
        prevTail.setNextOrdered(newTail);

        return true;
    }

    public E poll()
    {
        Object value = null;

        final Node node = head.next;

        if (null != node)
        {
            value = node.value;
            node.value = null;
            head = node;
        }

        return (E)value;
    }

    @SuppressWarnings("unchecked")
    private Node swapTail(final Node newTail)
    {
        return TAIL_UPD.getAndSet(this, newTail);
    }

    /**
     * Node with data.
     */
    private static class Node
    {
        public static final AtomicReferenceFieldUpdater<Node, Node> NODE_UPD =
            AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "next");

        Object value;
        volatile Node next;

        /**
         * Constructor.
         *
         * @param value Value.
         */
        private Node(Object value)
        {
            this.value = value;
        }

        /**
         * Set next node.
         *
         * @param next Next node.
         */
        void setNextOrdered(Node next)
        {
            NODE_UPD.lazySet(this, next);
        }
    }
}