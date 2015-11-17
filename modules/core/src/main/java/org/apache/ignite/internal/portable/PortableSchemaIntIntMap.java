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

package org.apache.ignite.internal.portable;

/**
 * Map for fast access to field order by ID.
 */
// TODO: IGNITE-1917: Inline into schema.
public class PortableSchemaIntIntMap {
    /** Minimum sensible size. */
    private static final int MIN_SIZE = 32;

    /** Empty cell. */
    private static final int EMPTY = 0;

    /** Data. */
    private final int[] data;

    /** Mask for index calculation. */
    private final int mask;

    /**
     * Constructor.
     *
     * @param vals Values.
     */
    public PortableSchemaIntIntMap(int[] vals) {
        int size = Math.max(nextPowerOfTwo(vals.length) << 2, MIN_SIZE);

        assert size > 0;

        ParseResult finalRes;

        ParseResult res1 = parse(vals, size);

        if (res1.collisions == 0)
            finalRes = res1;
        else {
            ParseResult res2 = parse(vals, size * 2);

            // Failed to decrease aom
            if (res2.collisions == 0)
                finalRes = res2;
            else
                finalRes = parse(vals, size * 4);
        }

        data = finalRes.data;

        mask = maskForPowerOfTwo(data.length / 2);
    }

    /**
     * Get order.
     *
     * @param id ID.
     * @return Order.
     */
    public int get(int id) {
        int idx = (id & mask) << 1;

        int curId = data[idx];

        if (id == curId) // Hit!
            return data[idx + 1];
        else if (curId == EMPTY) // No such ID!
            return PortableSchema.ORDER_NOT_FOUND;
        else {
            // Unlikely collision scenario.
            for (int i = 2; i < data.length; i += 2) {
                int newIdx = (idx + i) % data.length;

                assert newIdx < data.length - 1;

                curId = data[newIdx];

                if (id == curId)
                    return data[newIdx + 1];
                else if (curId == EMPTY)
                    return PortableSchema.ORDER_NOT_FOUND;
            }

            return PortableSchema.ORDER_NOT_FOUND;
        }
    }

    /**
     * Parse values.
     *
     * @param vals Values.
     * @param size Proposed result size.
     * @return Parse result.
     */
    private static ParseResult parse(int[] vals, int size) {
        int mask = maskForPowerOfTwo(size);

        int totalSize = size * 2;

        int[] data = new int[totalSize];
        int collisions = 0;

        for (int order = 0; order < vals.length; order++) {
            int id = vals[order];

            assert id != 0;

            int idIdx = (id & mask) << 1;

            if (data[idIdx] == 0) {
                // Found empty slot.
                data[idIdx] = id;
                data[idIdx + 1] = order;
            }
            else {
                // Collision!
                collisions++;

                boolean placeFound = false;

                for (int i = 2; i < totalSize; i += 2) {
                    int newIdIdx = (idIdx + i) % totalSize;

                    if (data[newIdIdx] == 0) {
                        data[newIdIdx] = id;
                        data[newIdIdx + 1] = order;

                        placeFound = true;

                        break;
                    }
                }

                assert placeFound : "Should always have a place for entry!";
            }
        }

        return new ParseResult(data, collisions);
    }

    /**
     * Get next power of two which greater or equal to the given number.
     * This implementation is not meant to be very efficient, so it is expected to be used relatively rare.
     *
     * @param val Number
     * @return Nearest pow2.
     */
    private static int nextPowerOfTwo(int val) {
        int res = 1;

        while (res < val)
            res = res << 1;

        if (res < 0)
            throw new IllegalArgumentException("Value is too big to find positive pow2: " + val);

        return res;
    }

    /**
     * Calculate mask for the given value which is a power of two.
     *
     * @param val Value.
     * @return Mask.
     */
    private static int maskForPowerOfTwo(int val) {
        int mask = 0;
        int comparand = 1;

        while (comparand < val) {
            mask |= comparand;

            comparand <<= 1;
        }

        return mask;
    }

    /**
     * Result of map parsing.
     */
    private static class ParseResult {
        /** Data. */
        public int[] data;

        /** Collisions. */
        public int collisions;

        /**
         * Constructor.
         *
         * @param data Data.
         * @param collisions Collisions.
         */
        public ParseResult(int[] data, int collisions) {
            this.data = data;
            this.collisions = collisions;
        }
    }
}
