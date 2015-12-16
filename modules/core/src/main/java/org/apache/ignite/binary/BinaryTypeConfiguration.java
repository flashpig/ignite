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

package org.apache.ignite.binary;

import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.BinaryConfiguration;

import java.io.Externalizable;

/**
 * Defines configuration properties for a specific binary type. Providing per-type
 * configuration is optional, as it is generally enough, and also optional, to provide global binary
 * configuration using {@link IgniteConfiguration#setBinaryConfiguration(BinaryConfiguration)}.
 * However, this class allows you to change configuration properties for a specific
 * binary type without affecting configuration for other binary types.
 */
public class BinaryTypeConfiguration {
    /** Default value of "ignore Java serialization" flag. */
    public static final boolean DFLT_IGNORE_JAVA_SER = false;

    /** Class name. */
    private String typeName;

    /** ID mapper. */
    private BinaryIdMapper idMapper;

    /** Serializer. */
    private BinarySerializer serializer;

    /** Enum flag. */
    private boolean isEnum;

    /** Ignore Java serialization flag. */
    private boolean ignoreJavaSer = DFLT_IGNORE_JAVA_SER;

    /**
     * Constructor.
     */
    public BinaryTypeConfiguration() {
        // No-op.
    }

    /**
     * Copying constructor.
     *
     * @param other Other instance.
     */
    public BinaryTypeConfiguration(BinaryTypeConfiguration other) {
        A.notNull(other, "other");

        typeName = other.typeName;
        idMapper = other.idMapper;
        serializer = other.serializer;
        isEnum = other.isEnum;
        ignoreJavaSer = other.ignoreJavaSer;
    }

    /**
     * @param typeName Class name.
     */
    public BinaryTypeConfiguration(String typeName) {
        this.typeName = typeName;
    }

    /**
     * Gets type name.
     *
     * @return Type name.
     */
    public String getTypeName() {
        return typeName;
    }

    /**
     * Sets type name.
     *
     * @param typeName Type name.
     */
    public void setTypeName(String typeName) {
        this.typeName = typeName;
    }

    /**
     * Gets ID mapper.
     *
     * @return ID mapper.
     */
    public BinaryIdMapper getIdMapper() {
        return idMapper;
    }

    /**
     * Sets ID mapper.
     *
     * @param idMapper ID mapper.
     */
    public void setIdMapper(BinaryIdMapper idMapper) {
        this.idMapper = idMapper;
    }

    /**
     * Gets serializer.
     *
     * @return Serializer.
     */
    public BinarySerializer getSerializer() {
        return serializer;
    }

    /**
     * Sets serializer.
     *
     * @param serializer Serializer.
     */
    public void setSerializer(BinarySerializer serializer) {
        this.serializer = serializer;
    }

    /**
     * Gets whether this is enum type.
     *
     * @return {@code True} if enum.
     */
    public boolean isEnum() {
        return isEnum;
    }

    /**
     * Sets whether this is enum type.
     *
     * @param isEnum {@code True} if enum.
     */
    public void setEnum(boolean isEnum) {
        this.isEnum = isEnum;
    }

    /**
     * Gets whether to ignore Java serialization mechanisms.
     * <p>
     * {@link BinaryMarshaller} allows for objects to be used without deserialization. To achieve this fields metadata
     * must be written along with their values. When custom Java serialization mechanics is present (such as
     * {@link Externalizable} or {@code writeObject()} method), Ignite has to respect it. But fields metadata cannot
     * be written in this case and so objects will be deserialized on the server.
     * <p>
     * To avoid deserialization on the server you can set this property to {@code true}. In this case Ignite will
     * ignore custom Java serialization and will write all objects fields (except of transient ones) directly.
     * <p>
     * Note that there are other ways to achieve the same things:
     * <ul>
     *     <li>Implement {@link Binarylizable} interface;</li>
     *     <li>Define custom {@link BinaryIdMapper} using {@link #setIdMapper(BinaryIdMapper)}.</li>
     * </ul>
     * Defaults to {@link #DFLT_IGNORE_JAVA_SER}.
     *
     * @return {@code True} if Java serialization should be ignored.
     */
    public boolean isIgnoreJavaSerialization() {
        return ignoreJavaSer;
    }

    /**
     * Sets whether to ignore Java serialization mechanisms. See {@link #isIgnoreJavaSerialization()} for details.
     *
     * @param ignoreJavaSer {@code True} if Java serialization should be ignored.
     */
    public void setIgnoreJavaSerialization(boolean ignoreJavaSer) {
        this.ignoreJavaSer = ignoreJavaSer;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryTypeConfiguration.class, this, super.toString());
    }
}