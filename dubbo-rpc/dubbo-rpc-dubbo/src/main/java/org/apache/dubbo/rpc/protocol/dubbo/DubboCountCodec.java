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

package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.Codec2;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.MultiMessage;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.FrameworkModel;

import java.io.IOException;

import static org.apache.dubbo.rpc.Constants.INPUT_KEY;
import static org.apache.dubbo.rpc.Constants.OUTPUT_KEY;

/**
 * 创建 ExchangeServer 时，使用的 Codec2 接口实现实际上是 DubboCountCodec
 */
public final class DubboCountCodec implements Codec2 {

    private DubboCodec codec;       // 编解码的能力都是 DubboCodec 提供的
    private FrameworkModel frameworkModel;

    public DubboCountCodec(FrameworkModel frameworkModel) {
        this.frameworkModel = frameworkModel;
        codec = new DubboCodec(frameworkModel);
    }

    @Override
    public void encode(Channel channel, ChannelBuffer buffer, Object msg) throws IOException {
        codec.encode(channel, buffer, msg);
    }

    @Override
    public Object decode(Channel channel, ChannelBuffer buffer) throws IOException {
        int save = buffer.readerIndex();                    // 首先保存 readerIndex 指针位置
        MultiMessage result = MultiMessage.create();        // 创建 MultiMessage 对象，其中可以存储多条消息
        do {        // 每次循环得到一条完整的消息
            Object obj = codec.decode(channel, buffer);   // 通过DubboCodec提供的解码能力解码一条消息
            if (Codec2.DecodeResult.NEED_MORE_INPUT == obj) {      // 如果可读字节数不足一条消息
                buffer.readerIndex(save);       // 重置readerIndex指针
                break;
            } else {
                result.addMessage(obj);     // 将成功解码的消息添加到 MultiMessage 中暂存
                logMessageLength(obj, buffer.readerIndex() - save);
                save = buffer.readerIndex();
            }
        } while (true);
        if (result.isEmpty()) {  // 一条消息也未解码出来，则返回 NEED_MORE_INPUT 错误码
            return Codec2.DecodeResult.NEED_MORE_INPUT;
        }
        if (result.size() == 1) {       // 只解码出来一条消息，则直接返回该条消息
            return result.get(0);
        }
        return result;         // 解码出多条消息的话，会将 MultiMessage 返回
    }

    private void logMessageLength(Object result, int bytes) {
        if (bytes <= 0) {
            return;
        }
        if (result instanceof Request) {
            try {
                ((RpcInvocation) ((Request) result).getData()).setAttachment(INPUT_KEY, String.valueOf(bytes));
            } catch (Throwable e) {
                /* ignore */
            }
        } else if (result instanceof Response) {
            try {
                ((AppResponse) ((Response) result).getResult()).setAttachment(OUTPUT_KEY, String.valueOf(bytes));
            } catch (Throwable e) {
                /* ignore */
            }
        }
    }

}
