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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.CacheObjectImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.marshaller.MarshallerExclusions;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.marshaller.portable.PortableMarshaller;
import org.jetbrains.annotations.Nullable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.Binarylizable;
import org.apache.ignite.binary.BinarySerializer;

import static java.lang.reflect.Modifier.isStatic;
import static java.lang.reflect.Modifier.isTransient;

/**
 * Portable class descriptor.
 */
public class PortableClassDescriptor {
    /** */
    private final PortableContext ctx;

    /** */
    private final Class<?> cls;

    /** */
    private final BinarySerializer serializer;

    /** ID mapper. */
    private final BinaryIdMapper idMapper;

    /** */
    private final Mode mode;

    /** */
    private final boolean userType;

    /** */
    private final int typeId;

    /** */
    private final String typeName;

    /** */
    private final Constructor<?> ctor;

    /** */
    private final BinaryFieldAccessor[] fields;

    /** */
    private final Method writeReplaceMtd;

    /** */
    private final Method readResolveMtd;

    /** */
    private final Map<String, String> fieldsMeta;

    /** */
    private final boolean keepDeserialized;

    /** */
    private final boolean registered;

    /** */
    private final boolean useOptMarshaller;

    /** */
    private final boolean excluded;

    /**
     * @param ctx Context.
     * @param cls Class.
     * @param userType User type flag.
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param idMapper ID mapper.
     * @param serializer Serializer.
     * @param metaDataEnabled Metadata enabled flag.
     * @param keepDeserialized Keep deserialized flag.
     * @param registered Whether typeId has been successfully registered by MarshallerContext or not.
     * @param predefined Whether the class is predefined or not.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    PortableClassDescriptor(
        PortableContext ctx,
        Class<?> cls,
        boolean userType,
        int typeId,
        String typeName,
        @Nullable BinaryIdMapper idMapper,
        @Nullable BinarySerializer serializer,
        boolean metaDataEnabled,
        boolean keepDeserialized,
        boolean registered,
        boolean predefined
    ) throws BinaryObjectException {
        assert ctx != null;
        assert cls != null;

        this.ctx = ctx;
        this.cls = cls;
        this.userType = userType;
        this.typeId = typeId;
        this.typeName = typeName;
        this.serializer = serializer;
        this.idMapper = idMapper;
        this.keepDeserialized = keepDeserialized;
        this.registered = registered;

        excluded = MarshallerExclusions.isExcluded(cls);

        useOptMarshaller = !predefined && initUseOptimizedMarshallerFlag();

        if (excluded)
            mode = Mode.EXCLUSION;
        else
            mode = serializer != null ? Mode.PORTABLE : PortableUtils.mode(cls);

        switch (mode) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case BOOLEAN:
            case DECIMAL:
            case STRING:
            case UUID:
            case DATE:
            case TIMESTAMP:
            case BYTE_ARR:
            case SHORT_ARR:
            case INT_ARR:
            case LONG_ARR:
            case FLOAT_ARR:
            case DOUBLE_ARR:
            case CHAR_ARR:
            case BOOLEAN_ARR:
            case DECIMAL_ARR:
            case STRING_ARR:
            case UUID_ARR:
            case DATE_ARR:
            case TIMESTAMP_ARR:
            case OBJ_ARR:
            case COL:
            case MAP:
            case MAP_ENTRY:
            case PORTABLE_OBJ:
            case ENUM:
            case ENUM_ARR:
            case CLASS:
            case EXCLUSION:
                ctor = null;
                fields = null;
                fieldsMeta = null;

                break;

            case PORTABLE:
            case EXTERNALIZABLE:
                ctor = constructor(cls);
                fields = null;
                fieldsMeta = null;

                break;

            case OBJECT:
                assert idMapper != null;

                ctor = constructor(cls);
                ArrayList<BinaryFieldAccessor> fields0 = new ArrayList<>();
                fieldsMeta = metaDataEnabled ? new HashMap<String, String>() : null;

                Collection<String> names = new HashSet<>();
                Collection<Integer> ids = new HashSet<>();

                for (Class<?> c = cls; c != null && !c.equals(Object.class); c = c.getSuperclass()) {
                    for (Field f : c.getDeclaredFields()) {
                        int mod = f.getModifiers();

                        if (!isStatic(mod) && !isTransient(mod)) {
                            f.setAccessible(true);

                            String name = f.getName();

                            if (!names.add(name))
                                throw new BinaryObjectException("Duplicate field name: " + name);

                            int fieldId = idMapper.fieldId(typeId, name);

                            if (!ids.add(fieldId))
                                throw new BinaryObjectException("Duplicate field ID: " + name);

                            BinaryFieldAccessor fieldInfo = BinaryFieldAccessor.create(f, fieldId);

                            fields0.add(fieldInfo);

                            if (metaDataEnabled)
                                fieldsMeta.put(name, fieldInfo.mode().typeName());
                        }
                    }
                }

                fields = fields0.toArray(new BinaryFieldAccessor[fields0.size()]);

                break;

            default:
                // Should never happen.
                throw new BinaryObjectException("Invalid mode: " + mode);
        }

        if (mode == Mode.PORTABLE || mode == Mode.EXTERNALIZABLE || mode == Mode.OBJECT) {
            readResolveMtd = U.findNonPublicMethod(cls, "readResolve");
            writeReplaceMtd = U.findNonPublicMethod(cls, "writeReplace");
        }
        else {
            readResolveMtd = null;
            writeReplaceMtd = null;
        }
    }

    /**
     * @return Described class.
     */
    Class<?> describedClass() {
        return cls;
    }

    /**
     * @return Type ID.
     */
    public int typeId() {
        return typeId;
    }

    /**
     * @return User type flag.
     */
    public boolean userType() {
        return userType;
    }

    /**
     * @return Fields meta data.
     */
    Map<String, String> fieldsMeta() {
        return fieldsMeta;
    }

    /**
     * @return Keep deserialized flag.
     */
    boolean keepDeserialized() {
        return keepDeserialized;
    }

    /**
     * @return Whether typeId has been successfully registered by MarshallerContext or not.
     */
    public boolean registered() {
        return registered;
    }

    /**
     * @return {@code true} if {@link OptimizedMarshaller} must be used instead of {@link PortableMarshaller}
     * for object serialization and deserialization.
     */
    public boolean useOptimizedMarshaller() {
        return useOptMarshaller;
    }

    /**
     * Checks whether the class values are explicitly excluded from marshalling.
     *
     * @return {@code true} if excluded, {@code false} otherwise.
     */
    public boolean excluded() {
        return excluded;
    }

    /**
     * Get ID mapper.
     *
     * @return ID mapper.
     */
    public BinaryIdMapper idMapper() {
        return idMapper;
    }

    /**
     * @return portableWriteReplace() method
     */
    @Nullable Method getWriteReplaceMethod() {
        return writeReplaceMtd;
    }

    /**
     * @return portableReadResolve() method
     */
    @Nullable Method getReadResolveMethod() {
        return readResolveMtd;
    }

    /**
     * @param obj Object.
     * @param writer Writer.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    void write(Object obj, BinaryWriterExImpl writer) throws BinaryObjectException {
        assert obj != null;
        assert writer != null;

        switch (mode) {
            case BYTE:
                writer.doWriteByte(GridPortableMarshaller.BYTE);
                writer.doWriteByte((byte)obj);

                break;

            case SHORT:
                writer.doWriteByte(GridPortableMarshaller.SHORT);
                writer.doWriteShort((short)obj);

                break;

            case INT:
                writer.doWriteByte(GridPortableMarshaller.INT);
                writer.doWriteInt((int)obj);

                break;

            case LONG:
                writer.doWriteByte(GridPortableMarshaller.LONG);
                writer.doWriteLong((long)obj);

                break;

            case FLOAT:
                writer.doWriteByte(GridPortableMarshaller.FLOAT);
                writer.doWriteFloat((float)obj);

                break;

            case DOUBLE:
                writer.doWriteByte(GridPortableMarshaller.DOUBLE);
                writer.doWriteDouble((double)obj);

                break;

            case CHAR:
                writer.doWriteByte(GridPortableMarshaller.CHAR);
                writer.doWriteChar((char)obj);

                break;

            case BOOLEAN:
                writer.doWriteByte(GridPortableMarshaller.BOOLEAN);
                writer.doWriteBoolean((boolean)obj);

                break;

            case DECIMAL:
                writer.doWriteDecimal((BigDecimal)obj);

                break;

            case STRING:
                writer.doWriteString((String)obj);

                break;

            case UUID:
                writer.doWriteUuid((UUID)obj);

                break;

            case DATE:
                writer.doWriteDate((Date)obj);

                break;

            case TIMESTAMP:
                writer.doWriteTimestamp((Timestamp)obj);

                break;

            case BYTE_ARR:
                writer.doWriteByteArray((byte[])obj);

                break;

            case SHORT_ARR:
                writer.doWriteShortArray((short[])obj);

                break;

            case INT_ARR:
                writer.doWriteIntArray((int[])obj);

                break;

            case LONG_ARR:
                writer.doWriteLongArray((long[])obj);

                break;

            case FLOAT_ARR:
                writer.doWriteFloatArray((float[])obj);

                break;

            case DOUBLE_ARR:
                writer.doWriteDoubleArray((double[])obj);

                break;

            case CHAR_ARR:
                writer.doWriteCharArray((char[])obj);

                break;

            case BOOLEAN_ARR:
                writer.doWriteBooleanArray((boolean[])obj);

                break;

            case DECIMAL_ARR:
                writer.doWriteDecimalArray((BigDecimal[])obj);

                break;

            case STRING_ARR:
                writer.doWriteStringArray((String[])obj);

                break;

            case UUID_ARR:
                writer.doWriteUuidArray((UUID[])obj);

                break;

            case DATE_ARR:
                writer.doWriteDateArray((Date[])obj);

                break;

            case TIMESTAMP_ARR:
                writer.doWriteTimestampArray((Timestamp[])obj);

                break;

            case OBJ_ARR:
                writer.doWriteObjectArray((Object[])obj);

                break;

            case COL:
                writer.doWriteCollection((Collection<?>)obj);

                break;

            case MAP:
                writer.doWriteMap((Map<?, ?>)obj);

                break;

            case MAP_ENTRY:
                writer.doWriteMapEntry((Map.Entry<?, ?>)obj);

                break;

            case ENUM:
                writer.doWriteEnum((Enum<?>)obj);

                break;

            case ENUM_ARR:
                writer.doWriteEnumArray((Object[])obj);

                break;

            case CLASS:
                writer.doWriteClass((Class)obj);

                break;

            case PORTABLE_OBJ:
                writer.doWritePortableObject((BinaryObjectImpl)obj);

                break;

            case PORTABLE:
                if (writeHeader(obj, writer)) {
                    try {
                        if (serializer != null)
                            serializer.writeBinary(obj, writer);
                        else
                            ((Binarylizable)obj).writeBinary(writer);

                        writer.postWrite(userType);
                    }
                    finally {
                        writer.popSchema();
                    }

                    if (obj.getClass() != BinaryMetaDataImpl.class
                        && ctx.isMetaDataChanged(typeId, writer.metaDataHashSum())) {
                        BinaryMetaDataCollector metaCollector = new BinaryMetaDataCollector(typeName);

                        if (serializer != null)
                            serializer.writeBinary(obj, metaCollector);
                        else
                            ((Binarylizable)obj).writeBinary(metaCollector);

                        ctx.updateMetaData(typeId, typeName, metaCollector.meta());
                    }
                }

                break;

            case EXTERNALIZABLE:
                if (writeHeader(obj, writer)) {
                    writer.rawWriter();

                    try {
                        ((Externalizable)obj).writeExternal(writer);

                        writer.postWrite(userType);
                    }
                    catch (IOException e) {
                        throw new BinaryObjectException("Failed to write Externalizable object: " + obj, e);
                    }
                    finally {
                        writer.popSchema();
                    }
                }

                break;

            case OBJECT:
                if (writeHeader(obj, writer)) {
                    try {
                        for (BinaryFieldAccessor info : fields)
                            info.write(obj, writer);

                        writer.postWrite(userType);
                    }
                    finally {
                        writer.popSchema();
                    }
                }

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }
    }

    /**
     * @param reader Reader.
     * @return Object.
     * @throws org.apache.ignite.binary.BinaryObjectException If failed.
     */
    Object read(BinaryReaderExImpl reader) throws BinaryObjectException {
        assert reader != null;

        Object res;

        switch (mode) {
            case PORTABLE:
                res = newInstance();

                reader.setHandler(res);

                if (serializer != null)
                    serializer.readBinary(res, reader);
                else
                    ((Binarylizable)res).readBinary(reader);

                break;

            case EXTERNALIZABLE:
                res = newInstance();

                reader.setHandler(res);

                try {
                    ((Externalizable)res).readExternal(reader);
                }
                catch (IOException | ClassNotFoundException e) {
                    throw new BinaryObjectException("Failed to read Externalizable object: " +
                        res.getClass().getName(), e);
                }

                break;

            case OBJECT:
                res = newInstance();

                reader.setHandler(res);

                for (BinaryFieldAccessor info : fields)
                    info.read(res, reader);

                break;

            default:
                assert false : "Invalid mode: " + mode;

                return null;
        }

        if (readResolveMtd != null) {
            try {
                res = readResolveMtd.invoke(res);

                reader.setHandler(res);
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            catch (InvocationTargetException e) {
                if (e.getTargetException() instanceof BinaryObjectException)
                    throw (BinaryObjectException)e.getTargetException();

                throw new BinaryObjectException("Failed to execute readResolve() method on " + res, e);
            }
        }

        return res;
    }

    /**
     * @param obj Object.
     * @param writer Writer.
     * @return Whether further write is needed.
     */
    private boolean writeHeader(Object obj, BinaryWriterExImpl writer) {
        if (writer.tryWriteAsHandle(obj))
            return false;

        PortableUtils.writeHeader(
            writer,
            userType,
            registered ? typeId : GridPortableMarshaller.UNREGISTERED_TYPE_ID,
            obj instanceof CacheObjectImpl ? 0 : obj.hashCode(),
            registered ? null : cls.getName()
        );

        return true;
    }

    /**
     * @return Instance.
     * @throws org.apache.ignite.binary.BinaryObjectException In case of error.
     */
    private Object newInstance() throws BinaryObjectException {
        assert ctor != null;

        try {
            return ctor.newInstance();
        }
        catch (InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new BinaryObjectException("Failed to instantiate instance: " + cls, e);
        }
    }

    /**
     * @param cls Class.
     * @return Constructor.
     * @throws org.apache.ignite.binary.BinaryObjectException If constructor doesn't exist.
     */
    @Nullable private static Constructor<?> constructor(Class<?> cls) throws BinaryObjectException {
        assert cls != null;

        try {
            Constructor<?> ctor = U.forceEmptyConstructor(cls);

            ctor.setAccessible(true);

            return ctor;
        }
        catch (IgniteCheckedException e) {
            throw new BinaryObjectException("Failed to get constructor for class: " + cls.getName(), e);
        }
    }

    /**
     * Determines whether to use {@link OptimizedMarshaller} for serialization or
     * not.
     *
     * @return {@code true} if to use, {@code false} otherwise.
     */
    private boolean initUseOptimizedMarshallerFlag() {
       boolean use;

        try {
            Method writeObj = cls.getDeclaredMethod("writeObject", ObjectOutputStream.class);
            Method readObj = cls.getDeclaredMethod("readObject", ObjectInputStream.class);

            if (!Modifier.isStatic(writeObj.getModifiers()) && !Modifier.isStatic(readObj.getModifiers()) &&
                writeObj.getReturnType() == void.class && readObj.getReturnType() == void.class)
                use = true;
            else
                use = false;
        }
        catch (NoSuchMethodException e) {
            use = false;
        }

        return use;
    }

    /** */
    enum Mode {
        /** Primitive byte. */
        P_BYTE("byte"),

        /** Primitive boolean. */
        P_BOOLEAN("boolean"),

        /** Primitive short. */
        P_SHORT("short"),

        /** Primitive char. */
        P_CHAR("char"),

        /** Primitive int. */
        P_INT("int"),

        /** Primitive long. */
        P_LONG("long"),

        /** Primitive float. */
        P_FLOAT("float"),

        /** Primitive int. */
        P_DOUBLE("double"),

        /** */
        BYTE("byte"),

        /** */
        SHORT("short"),

        /** */
        INT("int"),

        /** */
        LONG("long"),

        /** */
        FLOAT("float"),

        /** */
        DOUBLE("double"),

        /** */
        CHAR("char"),

        /** */
        BOOLEAN("boolean"),

        /** */
        DECIMAL("decimal"),

        /** */
        STRING("String"),

        /** */
        UUID("UUID"),

        /** */
        DATE("Date"),

        /** */
        TIMESTAMP("Timestamp"),

        /** */
        BYTE_ARR("byte[]"),

        /** */
        SHORT_ARR("short[]"),

        /** */
        INT_ARR("int[]"),

        /** */
        LONG_ARR("long[]"),

        /** */
        FLOAT_ARR("float[]"),

        /** */
        DOUBLE_ARR("double[]"),

        /** */
        CHAR_ARR("char[]"),

        /** */
        BOOLEAN_ARR("boolean[]"),

        /** */
        DECIMAL_ARR("decimal[]"),

        /** */
        STRING_ARR("String[]"),

        /** */
        UUID_ARR("UUID[]"),

        /** */
        DATE_ARR("Date[]"),

        /** */
        TIMESTAMP_ARR("Timestamp[]"),

        /** */
        OBJ_ARR("Object[]"),

        /** */
        COL("Collection"),

        /** */
        MAP("Map"),

        /** */
        MAP_ENTRY("Entry"),

        /** */
        PORTABLE_OBJ("Object"),

        /** */
        ENUM("Enum"),

        /** */
        ENUM_ARR("Enum[]"),

        /** */
        CLASS("Class"),

        /** */
        PORTABLE("Object"),

        /** */
        EXTERNALIZABLE("Object"),

        /** */
        OBJECT("Object"),

        /** */
        EXCLUSION("Exclusion");

        /** */
        private final String typeName;

        /**
         * @param typeName Type name.
         */
        Mode(String typeName) {
            this.typeName = typeName;
        }

        /**
         * @return Type name.
         */
        String typeName() {
            return typeName;
        }
    }
}
