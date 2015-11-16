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

package org.apache.ignite.internal.processors.cache.transactions;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteTxRemoteStateImpl implements IgniteTxState {
    /** Read set. */
    @GridToStringInclude
    protected Map<IgniteTxKey, IgniteTxEntry> readMap;

    /** Write map. */
    @GridToStringInclude
    protected Map<IgniteTxKey, IgniteTxEntry> writeMap;

    /**
     * @param readMap Read map.
     * @param writeMap Write map.
     */
    public IgniteTxRemoteStateImpl(Map<IgniteTxKey, IgniteTxEntry> readMap,
        Map<IgniteTxKey, IgniteTxEntry> writeMap) {
        this.readMap = readMap;
        this.writeMap = writeMap;
    }

    /** {@inheritDoc} */
    @Override public boolean implicitSingle() {
        return false;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Integer firstCacheId() {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void awaitLastFut(GridCacheSharedContext cctx) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public IgniteCheckedException validateTopology(GridCacheSharedContext cctx, GridDhtTopologyFuture topFut) {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean sync(GridCacheSharedContext cctx) {
        assert false;

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNearCache(GridCacheSharedContext cctx) {
        assert false;

        return false;
    }

    /** {@inheritDoc} */
    @Override public void addActiveCache(GridCacheContext cacheCtx, IgniteTxLocalAdapter tx)
        throws IgniteCheckedException {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public GridDhtTopologyFuture topologyReadLock(GridCacheSharedContext cctx, GridFutureAdapter<?> fut) {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void topologyReadUnlock(GridCacheSharedContext cctx) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public boolean storeUsed(GridCacheSharedContext cctx) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public Collection<CacheStoreManager> stores(GridCacheSharedContext cctx) {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onTxEnd(GridCacheSharedContext cctx, IgniteInternalTx tx, boolean commit) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry entry(IgniteTxKey key) {
        IgniteTxEntry e = writeMap == null ? null : writeMap.get(key);

        if (e == null)
            e = readMap == null ? null : readMap.get(key);

        return e;
    }

    /** {@inheritDoc} */
    @Override public boolean hasWriteKey(IgniteTxKey key) {
        return writeMap.containsKey(key);
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> readSet() {
        return readMap.keySet();
    }

    /** {@inheritDoc} */
    @Override public Set<IgniteTxKey> writeSet() {
        return writeMap.keySet();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> writeEntries() {
        return writeMap.values();
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> readEntries() {
        return readMap.values();
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> writeMap() {
        return writeMap;
    }

    /** {@inheritDoc} */
    @Override public Map<IgniteTxKey, IgniteTxEntry> readMap() {
        return readMap;
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return readMap.isEmpty() && writeMap.isEmpty();
    }

    /**
     * @param e Entry.
     */
    public void setWriteValue(IgniteTxEntry e) {
        IgniteTxEntry entry = writeMap.get(e.txKey());

        if (entry == null) {
            IgniteTxEntry rmv = readMap.remove(e.txKey());

            if (rmv != null) {
                e.cached(rmv.cached());

                writeMap.put(e.txKey(), e);
            }
            // If lock is explicit.
            else {
                e.cached(e.context().cache().entryEx(e.key()));

                // explicit lock.
                writeMap.put(e.txKey(), e);
            }
        }
        else {
            // Copy values.
            entry.value(e.value(), e.hasWriteValue(), e.hasReadValue());
            entry.entryProcessors(e.entryProcessors());
            entry.op(e.op());
            entry.ttl(e.ttl());
            entry.explicitVersion(e.explicitVersion());

            // Conflict resolution stuff.
            entry.conflictVersion(e.conflictVersion());
            entry.conflictExpireTime(e.conflictExpireTime());
        }
    }

    /**
     * @param key Key.
     * @param e Entry.
     */
    public void addWriteEntry(IgniteTxKey key, IgniteTxEntry e) {
        writeMap.put(key, e);
    }

    /**
     * Clears entry from transaction as it never happened.
     *
     * @param key key to be removed.
     */
    public void clearEntry(IgniteTxKey key) {
        readMap.remove(key);
        writeMap.remove(key);
    }

    /** {@inheritDoc} */
    @Override public Collection<IgniteTxEntry> allEntries() {
        return F.concat(false, writeEntries(), readEntries());
    }

    /** {@inheritDoc} */
    @Override public boolean init(int txSize) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean initialized() {
        assert false;

        return false;
    }

    /** {@inheritDoc} */
    @Override public void addEntry(IgniteTxEntry entry) {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public void seal() {
        assert false;
    }

    /** {@inheritDoc} */
    @Override public IgniteTxEntry singleWrite() {
        return null;
    }
}
