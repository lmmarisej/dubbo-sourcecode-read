/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dubbo.remoting.buffer;

import java.nio.ByteBuffer;

/**
 * ChannelBuffer 工具类。
 */
public final class ChannelBuffers {

    public static final ChannelBuffer EMPTY_BUFFER = new HeapChannelBuffer(0);

    public static final int DEFAULT_CAPACITY = 256;

    private ChannelBuffers() {
    }

    /**
     * 创建 DynamicChannelBuffer 对象，初始化大小由第一个参数指定，默认为 256。
     */
    public static ChannelBuffer dynamicBuffer() {
        return dynamicBuffer(DEFAULT_CAPACITY);
    }

    public static ChannelBuffer dynamicBuffer(int capacity) {
        return new DynamicChannelBuffer(capacity);
    }

    public static ChannelBuffer dynamicBuffer(int capacity,
                                              ChannelBufferFactory factory) {
        return new DynamicChannelBuffer(capacity, factory);
    }

    public static ChannelBuffer buffer(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException("capacity can not be negative");
        }
        if (capacity == 0) {
            return EMPTY_BUFFER;
        }
        return new HeapChannelBuffer(capacity);
    }

    public static ChannelBuffer wrappedBuffer(byte[] array, int offset, int length) {
        if (array == null) {
            throw new NullPointerException("array == null");
        }
        byte[] dest = new byte[length];
        System.arraycopy(array, offset, dest, 0, length);
        return wrappedBuffer(dest);
    }

    public static ChannelBuffer wrappedBuffer(byte[] array) {
        if (array == null) {
            throw new NullPointerException("array == null");
        }
        if (array.length == 0) {
            return EMPTY_BUFFER;
        }
        return new HeapChannelBuffer(array);        // 将传入的 byte[] 数字封装成 HeapChannelBuffer 对象。
    }

    public static ChannelBuffer wrappedBuffer(ByteBuffer buffer) {
        if (!buffer.hasRemaining()) {
            return EMPTY_BUFFER;
        }
        if (buffer.hasArray()) {
            return wrappedBuffer(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
        } else {
            return new ByteBufferBackedChannelBuffer(buffer);
        }
    }

    public static ChannelBuffer directBuffer(int capacity) {
        if (capacity == 0) {
            return EMPTY_BUFFER;
        }

        ChannelBuffer buffer = new ByteBufferBackedChannelBuffer(
            ByteBuffer.allocateDirect(capacity));       // 堆外内存
        buffer.clear();
        return buffer;
    }

    /**
     * 比较两个 ChannelBuffer 是否相同，其中会逐个比较两个 ChannelBuffer 中的前 7 个可读字节。
     */
    public static boolean equals(ChannelBuffer bufferA, ChannelBuffer bufferB) {
        final int aLen = bufferA.readableBytes();
        if (aLen != bufferB.readableBytes()) {
            return false;
        }

        final int byteCount = aLen & 7;

        int aIndex = bufferA.readerIndex();
        int bIndex = bufferB.readerIndex();

        for (int i = byteCount; i > 0; i--) {
            if (bufferA.getByte(aIndex) != bufferB.getByte(bIndex)) {
                return false;
            }
            aIndex++;
            bIndex++;
        }

        return true;
    }

    public static int hasCode(ChannelBuffer buffer) {
        final int aLen = buffer.readableBytes();
        final int byteCount = aLen & 7;

        int hashCode = 1;
        int arrayIndex = buffer.readerIndex();

        for (int i = byteCount; i > 0; i--) {
            hashCode = 31 * hashCode + buffer.getByte(arrayIndex++);
        }

        if (hashCode == 0) {
            hashCode = 1;
        }

        return hashCode;
    }

    /**
     * 用于比较两个 ChannelBuffer 的大小，会逐个比较两个 ChannelBuffer 中的全部可读字节
     */
    public static int compare(ChannelBuffer bufferA, ChannelBuffer bufferB) {
        final int aLen = bufferA.readableBytes();
        final int bLen = bufferB.readableBytes();
        final int minLength = Math.min(aLen, bLen);

        int aIndex = bufferA.readerIndex();
        int bIndex = bufferB.readerIndex();

        for (int i = minLength; i > 0; i--) {
            byte va = bufferA.getByte(aIndex);
            byte vb = bufferB.getByte(bIndex);
            if (va > vb) {
                return 1;
            } else if (va < vb) {
                return -1;
            }
            aIndex++;
            bIndex++;
        }

        return aLen - bLen;
    }

}
