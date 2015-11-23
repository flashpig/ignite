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

package org.apache.ignite.yardstick.h2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.yardstick.cache.model.Person;
import org.yardstickframework.BenchmarkConfiguration;
import org.yardstickframework.BenchmarkDriverAdapter;

import static org.yardstickframework.BenchmarkUtils.println;

/**
 *
 */
public class H2Benchmark extends BenchmarkDriverAdapter {
    /** Default DB options. */
    private static final String DB_OPTIONS = ";LOCK_MODE=3;MULTI_THREADED=1;DB_CLOSE_ON_EXIT=FALSE" +
        ";DEFAULT_LOCK_TIMEOUT=10000;FUNCTIONS_IN_SCHEMA=true;OPTIMIZE_REUSE_RESULTS=0;QUERY_CACHE_SIZE=0;" +
        "RECOMPILE_ALWAYS=1;MAX_OPERATION_MEMORY=0";

    /** */
    // TODO from arguments?!
    public static final double RANGE = 1_000_000;

    /** */
    private static final String CONNECTION_KEY = "CONNECTION_KEY";

    /** */
    private Connection[] conns;

    /** */
    private final AtomicInteger threadNum = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        conns = new Connection[cfg.threads()];

        for (int i = 0; i < conns.length; i++)
            conns[i] = openH2Connection(false);

        Connection conn = conns[0];

        initializeH2Schema(conn);

        println(cfg, "Populating query data...");

        long start = System.nanoTime();

        for (int i = 0; i < RANGE && !Thread.currentThread().isInterrupted(); i++) {
            insertInDb(conn, new Person(i, "firstName" + i, "lastName" + i, i * 1000));

            if (i % 100000 == 0)
                println(cfg, "Populated persons: " + i);
        }

        conn.commit();

        println(cfg, "Finished populating query data in " + ((System.nanoTime() - start) / 1_000_000) + " ms.");
    }

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        super.tearDown();

        for (Connection conn : conns)
            conn.close();
    }

    /**
     * Gets connection from a pool.
     *
     * @param autocommit {@code true} If connection should use autocommit mode.
     * @return Pooled connection.
     * @throws SQLException In case of error.
     */
    private static Connection openH2Connection(boolean autocommit) throws SQLException {
        System.setProperty("h2.serializeJavaObject", "false");
        System.setProperty("h2.objectCacheMaxPerElementSize", "0"); // Avoid ValueJavaObject caching.

        String dbName = "test";

        Connection conn = DriverManager.getConnection("jdbc:h2:mem:" + dbName + DB_OPTIONS);

        conn.setAutoCommit(autocommit);

        return conn;
    }

    /**
     * Initialize h2 database schema.
     *
     * @param conn Connection.
     * @throws SQLException If exception.
     */
    protected static void initializeH2Schema(Connection conn) throws SQLException {
        Statement st = conn.createStatement();

        st.execute("CREATE SCHEMA \"test\"");

        st.execute("create table \"test\".PERSON" +
            "  (_key other not null ," +
            "   _val other not null ," +
            "  id int unique, " +
            "  firstName varchar(255), " +
            "  lastName varchar(255)," +
            "  orgId int not null," +
            "  salary double)");

        st.execute("create INDEX person_salary_idx on \"test\".PERSON(salary)");

        conn.commit();
    }

    /**
     * @param p Person.
     * @throws SQLException If exception.
     */
    private static void insertInDb(Connection conn, Person p) throws SQLException {
        try(PreparedStatement st = conn.prepareStatement("insert into \"test\".PERSON " +
            "(_key, _val, id, orgId, firstName, lastName, salary) values(?, ?, ?, ?, ?, ?, ?)")) {
            st.setObject(1, p.getId(), Types.JAVA_OBJECT);
            st.setObject(2, p);
            st.setObject(3, p.getId());
            st.setObject(4, p.getOrganizationId());
            st.setObject(5, p.getFirstName());
            st.setObject(6, p.getLastName());
            st.setObject(7, p.getSalary());

            st.executeUpdate();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Connection conn = (Connection)ctx.get(CONNECTION_KEY);

        if (conn == null) {
            int num = threadNum.getAndIncrement();

            conn = conns[num];

            ctx.put(CONNECTION_KEY, conn);
        }

        double salary = ThreadLocalRandom.current().nextDouble() * RANGE * 1000;

        double maxSalary = salary + 1000;

        String qry = "select _key, _val from \"test\".PERSON where salary >= ? and salary <= ?";

        List<List<?>> lists = executeH2Query(conn, qry, salary, maxSalary);

        for (List<?> list : lists) {
            if (list.size() != 2)
                throw new Exception("List: " + list);

            Person p = (Person)list.get(1);

            if (p.getSalary() < salary || p.getSalary() > maxSalary)
                throw new Exception("Invalid person retrieved [min=" + salary + ", max=" + maxSalary +
                    ", person=" + p + ']');
        }

        return true;
    }

    /**
     * Execute SQL query on h2 database.
     *
     * @param sql SQL query.
     * @param args SQL arguments.
     * @return Result of SQL query on h2 database.
     * @throws SQLException If exception.
     */
    private static List<List<?>> executeH2Query(Connection conn, String sql, Object... args) throws SQLException {
        List<List<?>> res = new ArrayList<>();
        ResultSet rs = null;

        try(PreparedStatement st = conn.prepareStatement(sql)) {
            for (int idx = 0; idx < args.length; idx++)
                st.setObject(idx + 1, args[idx]);

            rs = st.executeQuery();

            ResultSetMetaData meta = rs.getMetaData();

            int colCnt = meta.getColumnCount();

            while (rs.next()) {
                List<Object> row = new ArrayList<>(colCnt);

                for (int i = 1; i <= colCnt; i++)
                    row.add(rs.getObject(i));

                res.add(row);
            }
        }
        finally {
            U.closeQuiet(rs);
        }

        return res;
    }
}