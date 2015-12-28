package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.cache.store.CacheStoreAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import javax.cache.processor.MutableEntry;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class IgniteCacheSyncUpdateNodeFailureTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        //ccfg.setCacheStoreFactory(new TestStoreFactory());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFullSyncOnePhaseCommit() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        Ignite ignite = ignite(0);

        final IgniteCache<Integer, Integer> cache = ignite.cache(null);

        final Integer key = nearKey(cache);

        final String backupNode = backupNode(key, null).name();

        Ignite primary = primaryNode(key, null);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start update.");

                cache.invoke(key, new TestEntryProcessor(Collections.singleton(backupNode)));

                Integer val = cache.get(key);

                log.info("End update, value: " + val);

                return null;
            }
        }, "put-thread");

        U.sleep(500);

        primary.close();

        fut.get();

        U.sleep(5000);

        log.info("Value at the end: " + cache.get(key));

        cache.put(key, 2);

        log.info("Value at the end: " + cache.get(key));
//        try (Transaction tx = ignite.transactions().txStart(PESSIMISTIC, REPEATABLE_READ)) {
//            cache.put(key, 0);
//
//            tx.commit();
//        }
    }

    /**
     * TODO: test with one backup and store, with two backups.
     *
     * @throws Exception If failed.
     */
    public void testFullSyncTxRecovery1() throws Exception {
        startGrids(4);

        awaitPartitionMapExchange();

        Ignite ignite = ignite(0);

        final IgniteCache<Integer, Integer> cache = ignite.cache(null);

        final Integer key = nearKey(cache);

        Ignite primary = primaryNode(key, null);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start update.");

                Set<String> backups = new HashSet<>();

                for (Ignite backup : backupNodes(key, null))
                    backups.add(backup.name());

                cache.invoke(key, new TestEntryProcessor(backups));

                log.info("End update, do get");

                Integer val = cache.get(key);

                log.info("End update, value: " + val);

                return null;
            }
        }, "put-thread");

        U.sleep(500);

        primary.close();

        U.sleep(5000);

        U.dumpThreads(log);

        fut.get();

        U.sleep(5000);

        log.info("Value at the end: " + cache.get(key));
    }

    /**
     * TODO: test with one backup and store, with two backups.
     *
     * @throws Exception If failed.
     */
    public void testFullSyncTxRecovery2() throws Exception {
        startGrids(4);

        awaitPartitionMapExchange();

        final Ignite ignite = ignite(0);

        final IgniteCache<Integer, Integer> cache = ignite.cache(null);

        final Integer key = backupKey(cache);

        Ignite primary = primaryNode(key, null);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start update.");

                Set<String> backups = new HashSet<>();

                for (Ignite backup : backupNodes(key, null))
                    backups.add(backup.name());

                cache.invoke(key, new TestEntryProcessor(backups));

                log.info("End update, do get");

                Integer val = cache.get(key);

                log.info("End update, value: " + val);

                return null;
            }
        }, "put-thread");

        U.sleep(500);

        primary.close();

        U.sleep(5000);

        U.dumpThreads(log);

        fut.get();

        U.sleep(5000);

        log.info("Value at the end: " + cache.get(key));
    }

    /**
     * TODO: test with one backup and store, with two backups.
     *
     * @throws Exception If failed.
     */
    public void testFullSyncTxRecovery3() throws Exception {
        startGrids(3);

        awaitPartitionMapExchange();

        final Ignite ignite = ignite(0);

        final IgniteCache<Integer, Integer> cache = ignite.cache(null);

        final Integer key = nearKey(cache);

        Ignite primary = primaryNode(key, null);

        IgniteInternalFuture<?> fut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                log.info("Start update.");

                cache.invoke(key, new TestEntryProcessor(Collections.singleton(backupNode(key, null).name())));

                log.info("End update, do get");

                Integer val = cache.get(key);

                log.info("End update, value: " + val);

                return null;
            }
        }, "put-thread");

        U.sleep(500);

        primary.close();

        //U.sleep(5000);
        //U.dumpThreads(log);

        fut.get();

        U.sleep(5000);

        log.info("Value at the end: " + cache.get(key));
    }

    /**
     *
     */
    static class TestEntryProcessor implements CacheEntryProcessor<Integer, Integer, Void> {
        /** */
        private Set<String> nodeNames;

        /**
         * @param nodeNames Node names where sleep will be called.
         */
        public TestEntryProcessor(Set<String> nodeNames) {
            this.nodeNames = nodeNames;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<Integer, Integer> entry, Object... args) {
            Ignite ignite = entry.unwrap(Ignite.class);

            if (nodeNames.contains(ignite.name())) {
                try {
                    System.out.println(Thread.currentThread().getName() + " sleep.");

                    Thread.sleep(10_000); // TODO use Latch

                    System.out.println(Thread.currentThread().getName() + " end sleep.");
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            entry.setValue(1);

            return null;
        }
    }

    /**
     *
     */
    private static class TestStoreFactory implements Factory<CacheStore> {
        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new CacheStoreAdapter() {
                @Override public Object load(Object key) throws CacheLoaderException {
                    return null;
                }

                @Override public void write(Cache.Entry entry) throws CacheWriterException {
                    // No-op.
                }

                @Override public void delete(Object key) throws CacheWriterException {
                    // No-op.
                }
            };
        }
    }
}
