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
package org.apache.dubbo.remoting;

import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

public class Dispatcher$Adaptive implements org.apache.dubbo.remoting.Dispatcher {
    public org.apache.dubbo.remoting.ChannelHandler dispatch(org.apache.dubbo.remoting.ChannelHandler arg0, org.apache.dubbo.common.URL arg1) {
        if (arg1 == null) throw new IllegalArgumentException("url == null");
        org.apache.dubbo.common.URL url = arg1;
        String extName = url.getParameter("dispatcher", url.getParameter("dispather", url.getParameter("channel.handler", "all")));
        if (extName == null)
            throw new IllegalStateException("Failed to get extension (org.apache.dubbo.remoting.Dispatcher) name from url (" + url.toString() + ") use keys([dispatcher, dispather, channel.handler])");
        ScopeModel scopeModel = ScopeModelUtil.getOrDefault(url.getScopeModel(), org.apache.dubbo.remoting.Dispatcher.class);
        org.apache.dubbo.remoting.Dispatcher extension = (org.apache.dubbo.remoting.Dispatcher) scopeModel.getExtensionLoader(org.apache.dubbo.remoting.Dispatcher.class).getExtension(extName);
        return extension.dispatch(arg0, arg1);
    }
}
