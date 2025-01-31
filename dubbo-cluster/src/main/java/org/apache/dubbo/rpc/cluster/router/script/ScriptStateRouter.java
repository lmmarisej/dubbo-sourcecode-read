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
package org.apache.dubbo.rpc.cluster.router.script;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.Holder;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.router.RouterSnapshotNode;
import org.apache.dubbo.rpc.cluster.router.state.AbstractStateRouter;
import org.apache.dubbo.rpc.cluster.router.state.BitList;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.CodeSource;
import java.security.Permissions;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_SCRIPT_TYPE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.FORCE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.RULE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.RUNTIME_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.TYPE_KEY;

/**
 * 支持 JDK 脚本引擎的所有脚本。
 *
 * 例如，JavaScript、JRuby、Groovy 等，通过 type=javascript 参数设置脚本类型，缺省为 javascript。
 */
public class ScriptStateRouter<T> extends AbstractStateRouter<T> {
    public static final String NAME = "SCRIPT_ROUTER";
    private static final int SCRIPT_ROUTER_DEFAULT_PRIORITY = 0;
    private static final Logger logger = LoggerFactory.getLogger(ScriptStateRouter.class);

    private static final Map<String, ScriptEngine> ENGINES = new ConcurrentHashMap<>(); // 按照脚本语言的类型复用 ScriptEngine 对象。

    private final ScriptEngine engine;      // 当前 ScriptRouter 使用的 ScriptEngine 对象。
    private final String rule;              // 当前 ScriptRouter 使用的具体脚本内容。
    private CompiledScript function;        // 根据 rule 这个具体脚本内容编译得到。

    private AccessControlContext accessControlContext;

    {
        //Just give permission of reflect to access member.
        Permissions perms = new Permissions();
        perms.add(new RuntimePermission("accessDeclaredMembers"));
        // Cast to Certificate[] required because of ambiguity:
        ProtectionDomain domain = new ProtectionDomain(new CodeSource(null, (Certificate[]) null), perms);
        accessControlContext = new AccessControlContext(new ProtectionDomain[]{domain});
    }

    public ScriptStateRouter(URL url) {
        super(url);
        this.setUrl(url);

        engine = getEngine(url);            // 根据 URL 中的 type 参数值，从 ENGINES 集合中获取对应的 ScriptEngine 对象
        rule = getRule(url);                // 获取 URL 中的 rule 参数值，即为具体的脚本
        try {
            Compilable compilable = (Compilable) engine;
            function = compilable.compile(rule);           // 编译 rule 字段中的脚本，得到 function 字段
        } catch (ScriptException e) {
            logger.error("route error, rule has been ignored. rule: " + rule +
                    ", url: " + RpcContext.getServiceContext().getUrl(), e);
        }
    }

    /**
     * get rule from url parameters.
     */
    private String getRule(URL url) {
        String vRule = url.getParameterAndDecoded(RULE_KEY);
        if (StringUtils.isEmpty(vRule)) {
            throw new IllegalStateException("route rule can not be empty.");
        }
        return vRule;
    }

    /**
     * create ScriptEngine instance by type from url parameters, then cache it
     */
    private ScriptEngine getEngine(URL url) {
        String type = url.getParameter(TYPE_KEY, DEFAULT_SCRIPT_TYPE_KEY);

        return ENGINES.computeIfAbsent(type, t -> {
            ScriptEngine scriptEngine = new ScriptEngineManager().getEngineByName(type);
            if (scriptEngine == null) {
                throw new IllegalStateException("unsupported route engine type: " + type);
            }
            return scriptEngine;
        });
    }

    @Override
    protected BitList<Invoker<T>> doRoute(BitList<Invoker<T>> invokers, URL url, Invocation invocation, boolean needToPrintMessage, Holder<RouterSnapshotNode<T>> nodeHolder, Holder<String> messageHolder) throws RpcException {
        if (engine == null || function == null) {
            if (needToPrintMessage) {
                messageHolder.set("Directly Return. Reason: engine or function is null");
            }
            return invokers;
        }
        Bindings bindings = createBindings(invokers, invocation);       // 创建Bindings对象作为function函数的入参
        // 调用function函数，并在getRoutedInvokers()方法中整理得到的Invoker集合
        return getRoutedInvokers(invokers, AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
            try {
                return function.eval(bindings);
            } catch (ScriptException e) {
                logger.error("route error, rule has been ignored. rule: " + rule + ", method:" +
                    invocation.getMethodName() + ", url: " + RpcContext.getContext().getUrl(), e);
                return invokers;
            }
        }, accessControlContext));
    }

    /**
     * get routed invokers from result of script rule evaluation
     */
    @SuppressWarnings("unchecked")
    protected BitList<Invoker<T>> getRoutedInvokers(BitList<Invoker<T>> invokers, Object obj) {
        BitList<Invoker<T>> result = invokers.clone();
        if (obj instanceof Invoker[]) {
            result.retainAll(Arrays.asList((Invoker<T>[]) obj));
        } else if (obj instanceof Object[]) {
            result.retainAll(Arrays.stream((Object[]) obj).map(item -> (Invoker<T>) item).collect(Collectors.toList()));
        } else {
            result.retainAll((List<Invoker<T>>) obj);
        }
        return result;
    }

    /**
     * create bindings for script engine
     */
    private Bindings createBindings(List<Invoker<T>> invokers, Invocation invocation) {
        Bindings bindings = engine.createBindings();
        // create a new List of invokers
        // 与前面的javascript的示例脚本结合，我们可以看到这里在Bindings中为脚本中的route()函数提供了invokers、Invocation、context三个参数
        bindings.put("invokers", new ArrayList<>(invokers));
        bindings.put("invocation", invocation);
        bindings.put("context", RpcContext.getClientAttachment());
        return bindings;
    }

    @Override
    public boolean isRuntime() {
        return this.getUrl().getParameter(RUNTIME_KEY, false);
    }

    @Override
    public boolean isForce() {
        return this.getUrl().getParameter(FORCE_KEY, false);
    }

}
