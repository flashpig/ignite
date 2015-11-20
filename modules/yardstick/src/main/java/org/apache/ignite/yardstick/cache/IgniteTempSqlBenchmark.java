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

package org.apache.ignite.yardstick.cache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.yardstick.cache.model.Person;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * -XX:+UnlockCommercialFeatures -XX:+FlightRecorder -XX:StartFlightRecording=delay=60s,duration=120s,filename=sql_benchmark_client1.jfr
 */
public class IgniteTempSqlBenchmark {
    public static void main(String[] args) throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();

        //cfg.setClientMode(true);

        cfg.setLocalHost("127.0.0.1");

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("127.0.0.1:47500","127.0.0.1:47501","127.0.0.1:47502"));

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setName("cache");
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(ATOMIC);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setIndexedTypes(Integer.class, Person.class);

        cfg.setCacheConfiguration(ccfg);

        final Ignite ignite = Ignition.start(cfg);

        final int QRY_THREADS = 1;
        final int PUT_THREADS = 0;

        final AtomicLong qryCntr = new AtomicLong();
        final AtomicLong putCntr = new AtomicLong();

        List<Thread> threads = new ArrayList<>();

        final IgniteCache<Integer, Person> cache = ignite.cache("cache");

        final int RANGE = 2 * 500_000;

        long start = System.nanoTime();

        try (IgniteDataStreamer<Integer, Person> dataLdr = ignite.dataStreamer(cache.getName())) {
            for (int i = 0; i < RANGE; i++) {
                dataLdr.addData(i, new Person(i, "firstName" + i, "lastName" + i, i * 1000));

                if (i % 100000 == 0)
                    System.out.println("Populated persons: " + i);
            }
        }

        System.out.println("Finished populating query data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");

        System.out.println("Plan : \n" + cache.query(
            new SqlFieldsQuery("explain select _val from Person where salary >= ? and salary <= ?")
                .setArgs(Integer.MIN_VALUE, Integer.MAX_VALUE)).getAll());

        System.out.println("Size : \n" + cache.query(
            new SqlFieldsQuery("select _val from Person where salary >= ? and salary <= ?")
                .setArgs(1000, 2000)).getAll().size());

        for (int i = 0; i < QRY_THREADS; i++) {
            Thread thread = new Thread() {
                public void run() {
                    System.out.println("Started thread");

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    while (true) {
                        double salary = rnd.nextDouble() * RANGE * 1000;

                        double maxSalary = salary + 1000;

                        Collection<Cache.Entry<Integer, Object>> entries = executeQuery(salary, maxSalary);

                        for (Cache.Entry<Integer, Object> entry : entries) {
                            Person p = (Person)entry.getValue();

                            if (p.getSalary() < salary || p.getSalary() > maxSalary) {
                                throw new RuntimeException("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                                    ", person=" + p + ']');
                            }
                        }

                        qryCntr.incrementAndGet();
                    }
                }

                /**
                 * @param minSalary Min salary.
                 * @param maxSalary Max salary.
                 * @return Query result.
                 */
                private Collection<Cache.Entry<Integer, Object>> executeQuery(double minSalary, double maxSalary) {
                    SqlQuery qry = new SqlQuery(Person.class, "salary >= ? and salary <= ?");

                    qry.setArgs(minSalary, maxSalary);

                    return cache.query(qry).getAll();
                }
            };

            threads.add(thread);
        }
        for (int i = 0; i < PUT_THREADS; i++) {
            Thread thread = new Thread() {
                public void run() {
                    System.out.println("Started thread");

                    ThreadLocalRandom rnd = ThreadLocalRandom.current();

                    List<ConcurrentSkipListMap<Integer, Person>> maps = new ArrayList<>();

//                    for (int i = 0; i < 4; i++)
//                        maps.add(new ConcurrentSkipListMap<Integer, Person>());
//
//                    while (true) {
//                        Integer k = rnd.nextInt(RANGE);
//
//                        Person p = new Person(k, "firstName" + k, "lastName" + k, k * 1000);
//
//                        for (int i = 0;  i < maps.size(); i++) {
//                            ConcurrentSkipListMap<Integer, Person> cache = maps.get(i);
//
//                            cache.put(k, p);
//                        }
//
//                        putCntr.incrementAndGet();
//                    }

                    while (true) {
                        Integer k = rnd.nextInt(RANGE);

                        Person p = new Person(k, "firstName" + k, "lastName" + k, k * 1000);

                        cache.put(k, p);

                        putCntr.incrementAndGet();
                    }


                }
            };

            threads.add(thread);
        }

        for (Thread thread : threads)
            thread.start();

        Thread.sleep(10_000);

        System.out.println("Warmup finished");

        long total = 0;
        long total2 = 0;
        long cnt = 0;

        long endTime = System.currentTimeMillis() + 90_000;

        while (System.currentTimeMillis() < endTime) {
            long c1 = qryCntr.get();
            long p1 = putCntr.get();

            Thread.sleep(5000);

            long c2 = qryCntr.get();
            long p2 = putCntr.get();

            long ops = (long)((c2 - c1) / 5f);
            long ops2 = (long)((p2 - p1) / 5f);

            total += ops;
            total2 += ops2;
            cnt++;

            System.out.println("Queries: " + ops + " Puts: " + ops2);
        }

        System.out.println("Result: " + total / (float)cnt + " " + total2 / (float)cnt);

        //ignite.compute().run(new Exit());

        System.exit(0);
    }
}
