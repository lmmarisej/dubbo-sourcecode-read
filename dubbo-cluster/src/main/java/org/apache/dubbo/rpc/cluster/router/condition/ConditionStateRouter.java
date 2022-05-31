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
package org.apache.dubbo.rpc.cluster.router.condition;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.Holder;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Constants;
import org.apache.dubbo.rpc.cluster.router.RouterSnapshotNode;
import org.apache.dubbo.rpc.cluster.router.condition.config.AppStateRouter;
import org.apache.dubbo.rpc.cluster.router.state.AbstractStateRouter;
import org.apache.dubbo.rpc.cluster.router.state.BitList;

import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.dubbo.common.constants.CommonConstants.ENABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.HOST_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METHODS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METHOD_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.ADDRESS_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.FORCE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.RULE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.RUNTIME_KEY;

/**
 * ConditionRouter
 * It supports the conditional routing configured by "override://", in 2.6.x,
 * refer to https://dubbo.apache.org/en/docs/v2.7/user/examples/routing-rule/ .
 * For 2.7.x and later, please refer to {@link org.apache.dubbo.rpc.cluster.router.condition.config.ServiceRouter}
 * and {@link AppStateRouter}
 * refer to https://dubbo.apache.org/zh/docs/v2.7/user/examples/routing-rule/ .
 */
public class ConditionStateRouter<T> extends AbstractStateRouter<T> {
    public static final String NAME = "condition";

    private static final Logger logger = LoggerFactory.getLogger(ConditionStateRouter.class);
    protected static final Pattern ROUTE_PATTERN = Pattern.compile("([&!=,]*)\\s*([^&!=,\\s]+)");   // 切分路由规则的正则表达式。
    protected static Pattern ARGUMENTS_PATTERN = Pattern.compile("arguments\\[(\\d+)]");
    protected Map<String, MatchPair> whenCondition; // Consumer 匹配的条件集合，通过解析条件表达式 rule 的 => 之前半部分，可以得到该集合中的内容。
    protected Map<String, MatchPair> thenCondition; // Provider 匹配的条件集合，通过解析条件表达式 rule 的 => 之后半部分，可以得到该集合中的内容。

    private boolean enabled;

    public ConditionStateRouter(URL url, String rule, boolean force, boolean enabled) {
        super(url);
        this.setForce(force);
        this.enabled = enabled;
        if (enabled) {
            this.init(rule);
        }
    }

    public ConditionStateRouter(URL url) {
        super(url);
        this.setUrl(url);
        this.setForce(url.getParameter(FORCE_KEY, false));
        this.enabled = url.getParameter(ENABLED_KEY, true);
        if (enabled) {
            init(url.getParameterAndDecoded(RULE_KEY));
        }
    }

    /**
     * 据 URL 中携带的相应参数初始化 priority、force、enable 等字段，然后从 URL 的 rule 参数中获取路由规则进行解析
     */
    public void init(String rule) {
        try {
            if (rule == null || rule.trim().length() == 0) {
                throw new IllegalArgumentException("Illegal route rule!");
            }
            // 将路由规则中的"consumer."和"provider."字符串清理掉
            rule = rule.replace("consumer.", "").replace("provider.", "");

            int i = rule.indexOf("=>");    // 按照"=>"字符串进行分割，得到whenRule和thenRule两部分
            String whenRule = i < 0 ? null : rule.substring(0, i).trim();
            String thenRule = i < 0 ? rule.trim() : rule.substring(i + 2).trim();

            // 解析whenRule和thenRule，得到whenCondition和thenCondition两个条件集合
            Map<String, MatchPair> when = StringUtils.isBlank(whenRule) || "true".equals(whenRule) ? new HashMap<String, MatchPair>() : parseRule(whenRule);
            Map<String, MatchPair> then = StringUtils.isBlank(thenRule) || "false".equals(thenRule) ? null : parseRule(thenRule);
            // NOTE: It should be determined on the business level whether the `When condition` can be empty or not.
            this.whenCondition = when;
            this.thenCondition = then;
        } catch (ParseException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 解析一条完整的条件表达式，生成对应 MatchPair
     *
     * host = 2.2.2.2,1.1.1.1,3.3.3.3 & method !=get => host = 1.2.3.4
     */
    private static Map<String, MatchPair> parseRule(String rule) throws ParseException {
        Map<String, MatchPair> condition = new HashMap<>();
        if (StringUtils.isBlank(rule)) {
            return condition;
        }
        // Key-Value pair, stores both match and mismatch conditions
        MatchPair pair = null;
        // Multiple values
        Set<String> values = null;
        final Matcher matcher = ROUTE_PATTERN.matcher(rule);            // 首先，按照ROUTE_PATTERN指定的正则表达式匹配整个条件表达式
        while (matcher.find()) { // Try to match one by one      // 遍历匹配的结果
            String separator = matcher.group(1);         // 每个匹配结果有两部分(分组)，第一部分是分隔符，第二部分是内容
            String content = matcher.group(2);
            // Start part of the condition expression.
            if (StringUtils.isEmpty(separator)) {        // ---(1) 没有分隔符，content即为参数名称
                pair = new MatchPair();
                // 初始化MatchPair对象，并将其与对应的Key(即content)记录到condition集合中
                condition.put(content, pair);
            }
            // The KV part of the condition expression
            else if ("&".equals(separator)) {        // ---(4)
                // &分隔符表示多个表达式,会创建多个MatchPair对象
                if (condition.get(content) == null) {
                    pair = new MatchPair();
                    condition.put(content, pair);
                } else {
                    pair = condition.get(content);
                }
            }
            // The Value in the KV part.
            else if ("=".equals(separator)) {       // ---(2)
                // =以及!=两个分隔符表示KV的分界线
                if (pair == null) {
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                }

                values = pair.matches;
                values.add(content);
            }
            // The Value in the KV part.
            else if ("!=".equals(separator)) {      // ---(5)
                if (pair == null) {
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                }

                values = pair.mismatches;
                values.add(content);
            }
            // The Value in the KV part, if Value have more than one items.
            else if (",".equals(separator)) { // Should be separated by ','  // ---(3)
                // 逗号分隔符表示有多个Value值
                if (values == null || values.isEmpty()) {
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                }
                values.add(content);
            } else {
                throw new ParseException("Illegal route rule \"" + rule
                        + "\", The error char '" + separator + "' at index "
                        + matcher.start() + " before \"" + content + "\".", matcher.start());
            }
        }
        return condition;
    }

    @Override
    protected BitList<Invoker<T>> doRoute(BitList<Invoker<T>> invokers, URL url, Invocation invocation,
                                          boolean needToPrintMessage, Holder<RouterSnapshotNode<T>> nodeHolder,
                                          Holder<String> messageHolder) throws RpcException {
        if (!enabled) {     // 通过enable字段判断当前ConditionRouter对象是否可用
            if (needToPrintMessage) {
                messageHolder.set("Directly return. Reason: ConditionRouter disabled.");
            }
            return invokers;
        }

        if (CollectionUtils.isEmpty(invokers)) {        // 当前invokers集合为空，则直接返回
            if (needToPrintMessage) {
                messageHolder.set("Directly return. Reason: Invokers from previous router is empty.");
            }
            return invokers;
        }
        try {
            if (!matchWhen(url, invocation)) {       // 判断=>之后是否存在Provider过滤条件，若不存在则直接返回空集合，表示无Provider可用
                if (needToPrintMessage) {
                    messageHolder.set("Directly return. Reason: WhenCondition not match.");
                }
                return invokers;
            }
            if (thenCondition == null) {
                logger.warn("The current consumer in the service blacklist. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey());
                if (needToPrintMessage) {
                    messageHolder.set("Empty return. Reason: ThenCondition is empty.");
                }
                return BitList.emptyList();
            }
            BitList<Invoker<T>> result = invokers.clone();
            result.removeIf(invoker -> !matchThen(invoker.getUrl(), url));

            if (!result.isEmpty()) {
                if (needToPrintMessage) {
                    messageHolder.set("Match return.");
                }
                return result;
            } else if (this.isForce()) {        // 在无Invoker符合条件时，根据force决定是返回空集合还是返回全部Invoker
                logger.warn("The route result is empty and force execute. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey() + ", router: " + url.getParameterAndDecoded(RULE_KEY));

                if (needToPrintMessage) {
                    messageHolder.set("Empty return. Reason: Empty result from condition and condition is force.");
                }
                return result;
            }
        } catch (Throwable t) {
            logger.error("Failed to execute condition router rule: " + getUrl() + ", invokers: " + invokers + ", cause: " + t.getMessage(), t);
        }
        if (needToPrintMessage) {
            messageHolder.set("Directly return. Reason: Error occurred ( or result is empty ).");
        }
        return invokers;
    }

    @Override
    public boolean isRuntime() {
        // We always return true for previously defined Router, that is, old Router doesn't support cache anymore.
//        return true;
        return this.getUrl().getParameter(RUNTIME_KEY, false);
    }

    boolean matchWhen(URL url, Invocation invocation) {
        return CollectionUtils.isEmptyMap(whenCondition) || matchCondition(whenCondition, url, null, invocation);
    }

    private boolean matchThen(URL url, URL param) {
        return CollectionUtils.isNotEmptyMap(thenCondition) && matchCondition(thenCondition, url, param, null);
    }

    private boolean matchCondition(Map<String, MatchPair> condition, URL url, URL param, Invocation invocation) {
        Map<String, String> sample = url.toMap();
        boolean result = false;
        for (Map.Entry<String, MatchPair> matchPair : condition.entrySet()) {
            String key = matchPair.getKey();

            if (key.startsWith(Constants.ARGUMENTS)) {
                if (!matchArguments(matchPair, invocation)) {
                    return false;
                } else {
                    result = true;
                    continue;
                }
            }

            String sampleValue;
            //get real invoked method name from invocation
            if (invocation != null && (METHOD_KEY.equals(key) || METHODS_KEY.equals(key))) {
                sampleValue = invocation.getMethodName();
            } else if (ADDRESS_KEY.equals(key)) {
                sampleValue = url.getAddress();
            } else if (HOST_KEY.equals(key)) {
                sampleValue = url.getHost();
            } else {
                sampleValue = sample.get(key);
            }
            if (sampleValue != null) {
                if (!matchPair.getValue().isMatch(sampleValue, param)) {
                    return false;
                } else {
                    result = true;
                }
            } else {
                //not pass the condition
                if (!matchPair.getValue().matches.isEmpty()) {
                    return false;
                } else {
                    result = true;
                }
            }
        }
        return result;
    }

    /**
     * analysis the arguments in the rule.
     * Examples would be like this:
     * "arguments[0]=1", whenCondition is that the first argument is equal to '1'.
     * "arguments[1]=a", whenCondition is that the second argument is equal to 'a'.
     * @param matchPair
     * @param invocation
     * @return
     */
    public boolean matchArguments(Map.Entry<String, MatchPair> matchPair, Invocation invocation) {
        try {
            // split the rule
            String key = matchPair.getKey();
            String[] expressArray = key.split("\\.");
            String argumentExpress = expressArray[0];
            final Matcher matcher = ARGUMENTS_PATTERN.matcher(argumentExpress);
            if (!matcher.find()) {
                return false;
            }

            //extract the argument index
            int index = Integer.parseInt(matcher.group(1));
            if (index < 0 || index > invocation.getArguments().length) {
                return false;
            }

            //extract the argument value
            Object object = invocation.getArguments()[index];

            if (matchPair.getValue().isMatch(String.valueOf(object), null)) {
                return true;
            }
        } catch (Exception e) {
            logger.warn("Arguments match failed, matchPair[]" + matchPair + "] invocation[" + invocation + "]", e);
        }

        return false;
    }

    protected static final class MatchPair {
        // 当 matches 集合为空的时候，会逐个遍历 mismatches 集合中的匹配条件，匹配成功任意一条即会返回 false。
        final Set<String> matches = new HashSet<>();
        // 为空的时候，会逐个遍历 matches 集合中的匹配条件，匹配成功任意一条即会返回 true。
        final Set<String> mismatches = new HashSet<>();

        private boolean isMatch(String value, URL param) {
            if (!matches.isEmpty() && mismatches.isEmpty()) {
                for (String match : matches) {
                    if (UrlUtils.isMatchGlobPattern(match, value, param)) {
                        return true;
                    }
                }
                return false;
            }

            if (!mismatches.isEmpty() && matches.isEmpty()) {
                for (String mismatch : mismatches) {
                    if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {
                        return false;
                    }
                }
                return true;
            }

            /*
            当 matches 集合和 mismatches 集合同时不为空时，会优先匹配 mismatches 集合中的条件，成功匹配任意一条规则，就会返回 false；
            若 mismatches 中的条件全部匹配失败，才会开始匹配 matches 集合，成功匹配任意一条规则，就会返回 true。
             */
            if (!matches.isEmpty() && !mismatches.isEmpty()) {
                //when both mismatches and matches contain the same value, then using mismatches first
                for (String mismatch : mismatches) {
                    if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {
                        return false;
                    }
                }
                for (String match : matches) {
                    if (UrlUtils.isMatchGlobPattern(match, value, param)) {
                        return true;
                    }
                }
                return false;
            }
            return false;       // 都没有成功匹配时，直接返回 false。
        }
    }
}
