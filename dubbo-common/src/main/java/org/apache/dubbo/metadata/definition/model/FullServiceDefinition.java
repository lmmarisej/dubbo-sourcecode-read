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
package org.apache.dubbo.metadata.definition.model;

import java.util.Map;

/**
 * 在服务发布的时候，会将服务的 URL 中的部分数据封装为 FullServiceDefinition 对象，然后作为元数据存储起来。
 *
 * FullServiceDefinition 继承了 ServiceDefinition，并在 ServiceDefinition 基础之上扩展了 params 集合（Map<String, String> 类型），
 * 用来存储 URL 上的参数。
 */
public class FullServiceDefinition extends ServiceDefinition {

    private Map<String, String> parameters;

    public Map<String, String> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    @Override
    public String toString() {
        return "FullServiceDefinition{" +
                "parameters=" + parameters +
                "} " + super.toString();
    }

}
