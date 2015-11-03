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

import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerContextTestImpl;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.apache.ignite.portable.PortableField;
import org.apache.ignite.portable.PortableMetadata;
import org.apache.ignite.portable.PortableTypeConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.Arrays;

/**
 * Contains tests for portable object fields.
 */
public abstract class PortableFieldsAbstractSelfTest extends GridCommonAbstractTest {
    /** Dummy metadata handler. */
    protected static final PortableMetaDataHandler META_HND = new PortableMetaDataHandler() {
        @Override public void addMeta(int typeId, PortableMetadata meta) {
            // No-op.
        }

        @Override public PortableMetadata metadata(int typeId) {
            return null;
        }
    };

    /** Marshaller. */
    protected PortableMarshaller marsh;

    /** Portable context. */
    protected PortableContext ctx;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        ctx = new PortableContext(META_HND, null);

        marsh = new PortableMarshaller();

        marsh.setTypeConfigurations(Arrays.asList(
            new PortableTypeConfiguration(TestObject.class.getName()),
            new PortableTypeConfiguration(TestOuterObject.class.getName())
        ));

        marsh.setContext(new MarshallerContextTestImpl(null));

        IgniteUtils.invoke(PortableMarshaller.class, marsh, "setPortableContext", ctx);
    }

    /**
     * Test byte field.
     *
     * @throws Exception If failed.
     */
    public void testByte() throws Exception {
        check("fByte");
    }

    /**
     * Test boolean field.
     *
     * @throws Exception If failed.
     */
    public void testBoolean() throws Exception {
        check("fBool");
    }

    /**
     * Test short field.
     *
     * @throws Exception If failed.
     */
    public void testShort() throws Exception {
        check("fShort");
    }

    /**
     * Test char field.
     *
     * @throws Exception If failed.
     */
    public void testChar() throws Exception {
        check("fChar");
    }
    /**
     * Test int field.
     *
     * @throws Exception If failed.
     */
    public void testInt() throws Exception {
        check("fInt");
    }

    /**
     * Test long field.
     *
     * @throws Exception If failed.
     */
    public void testLong() throws Exception {
        check("fLong");
    }

    /**
     * Test int field.
     *
     * @throws Exception If failed.
     */
    public void testFloat() throws Exception {
        check("fFloat");
    }
    /**
     * Test int field.
     *
     * @throws Exception If failed.
     */
    public void testDouble() throws Exception {
        check("fDouble");
    }

    /**
     * Check field resolution in both normal and nested modes.
     *
     * @param fieldName Field name.
     * @throws Exception If failed.
     */
    public void check(String fieldName) throws Exception {
        checkNormal(fieldName);
        checkNested(fieldName);
    }

    /**
     * Check field.
     *
     * @param fieldName Field name.
     * @throws Exception If failed.
     */
    private void checkNormal(String fieldName) throws Exception {
        TestContext ctx = context(fieldName);

        check0(fieldName, ctx);
    }

    /**
     * Check nested field.
     *
     * @param fieldName Field name.
     * @throws Exception If failed.
     */
    private void checkNested(String fieldName) throws Exception {
        TestContext ctx = nestedContext(fieldName);

        check0(fieldName, ctx);
    }

    /**
     * Internal check routine.
     *
     * @param fieldName Field name.
     * @param ctx Context.
     * @throws Exception If failed.
     */
    private void check0(String fieldName, TestContext ctx) throws Exception {
        Object expVal = U.field(ctx.obj, fieldName);

        assertTrue(ctx.field.exists(ctx.portObj));

        assertEquals(expVal, ctx.field.value(ctx.portObj));
    }

    /**
     * Get test context.
     *
     * @param fieldName Field name.
     * @return Test context.
     * @throws Exception If failed.
     */
    private TestContext context(String fieldName) throws Exception {
        TestObject obj = createObject();

        PortableObjectEx portObj = toPortable(marsh, obj);

        PortableField field = portObj.fieldDescriptor(fieldName);

        return new TestContext(obj, portObj, field);
    }

    /**
     * Get test context with nested test object.
     *
     * @param fieldName Field name.
     * @return Test context.
     * @throws Exception If failed.
     */
    private TestContext nestedContext(String fieldName) throws Exception {
        TestObject obj = createObject();
        TestOuterObject outObj = new TestOuterObject(obj);

        PortableObjectEx portOutObj = toPortable(marsh, outObj);
        PortableObjectEx portObj = portOutObj.field("fInner");

        assert portObj != null;

        PortableField field = portObj.fieldDescriptor(fieldName);

        return new TestContext(obj, portObj, field);
    }

    /**
     * Create test object.
     *
     * @return Test object.
     */
    private TestObject createObject() {
        return new TestObject(0);
    }

    /**
     * Convert object to portable object.
     *
     * @param marsh Marshaller.
     * @param obj Object.
     * @return Portable object.
     * @throws Exception If failed.
     */
    protected abstract PortableObjectEx toPortable(PortableMarshaller marsh, Object obj) throws Exception;

    /**
     * Outer test object.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static class TestOuterObject {
        /** Inner object. */
        public TestObject fInner;

        /**
         * Default constructor.
         */
        public TestOuterObject() {
            // No-op.
        }

        /**
         * Constructor.
         *
         * @param fInner Inner object.
         */
        public TestOuterObject(TestObject fInner) {
            this.fInner = fInner;
        }
    }

    /**
     * Test object class, c
     */
    @SuppressWarnings("UnusedDeclaration")
    public static class TestObject {
        /** Primitive fields. */
        public byte fByte;
        public boolean fBool;
        public short fShort;
        public char fChar;
        public int fInt;
        public long fLong;
        public float fFloat;
        public double fDouble;

        /**
         * Default constructor.
         */
        public TestObject() {
            // No-op.
        }

        /**
         * Non-default constructor.
         *
         * @param ignore Ignored.
         */
        public TestObject(int ignore) {
            fByte = 1;
            fBool = true;
            fShort = 2;
            fChar = 3;
            fInt = 4;
            fLong = 5;
            fFloat = 6.6f;
            fDouble = 7.7;
        }
    }

    /**
     * Test context.
     */
    public static class TestContext {
        /** Object. */
        public final TestObject obj;

        /** Portable object. */
        public final PortableObjectEx portObj;

        /** Field. */
        public final PortableField field;

        /**
         * Constructor.
         *
         * @param obj Object.
         * @param portObj Portable object.
         * @param field Field.
         */
        public TestContext(TestObject obj, PortableObjectEx portObj, PortableField field) {
            this.obj = obj;
            this.portObj = portObj;
            this.field = field;
        }
    }
}
