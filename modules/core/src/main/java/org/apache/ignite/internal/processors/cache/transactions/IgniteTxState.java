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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTopologyFuture;
import org.apache.ignite.internal.processors.cache.store.CacheStoreManager;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public interface IgniteTxState {
    public boolean implicitSingle();

    @Nullable public Integer firstCacheId();

    public void awaitLastFut(GridCacheSharedContext cctx);

    public IgniteCheckedException validateTopology(GridCacheSharedContext cctx, GridDhtTopologyFuture topFut);

    public boolean sync(GridCacheSharedContext cctx);

    public boolean hasNearCache(GridCacheSharedContext cctx);

    public void addActiveCache(GridCacheContext cacheCtx, IgniteTxLocalAdapter tx) throws IgniteCheckedException;

    public GridDhtTopologyFuture topologyReadLock(GridCacheSharedContext cctx, GridFutureAdapter<?> fut);

    public void topologyReadUnlock(GridCacheSharedContext cctx);

    public boolean storeUsed(GridCacheSharedContext cctx);

    public Collection<CacheStoreManager> stores(GridCacheSharedContext cctx);

    public void onTxEnd(GridCacheSharedContext cctx, IgniteInternalTx tx, boolean commit);

    // public IgniteTxEntry entry(IgniteTxKey key);
}
