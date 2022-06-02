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
package org.apache.dubbo.config.context;

import org.apache.dubbo.common.config.Configuration;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.AbstractConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * AbstractConfig 与 Configuration 之间的适配器，将 AbstractConfig 对象转换成 Configuration 对象。
 *
 * This class receives an {@link AbstractConfig} and exposes its attributes through {@link Configuration}
 */
public class ConfigConfigurationAdapter implements Configuration {

    private Map<String, String> metaData;    // 获取该 AbstractConfig 对象中的全部字段与字段值的映射

    public ConfigConfigurationAdapter(AbstractConfig config, String prefix) {
        Map<String, String> configMetadata = config.getMetaData();
        if (StringUtils.hasText(prefix)) {
            metaData = new HashMap<>(configMetadata.size(), 1.0f);
            // 根据 AbstractConfig 配置的 prefix 和 id，修改 metaData 集合中 Key 的名称
            for (Map.Entry<String, String> entry : configMetadata.entrySet()) {
                metaData.put(prefix + "." + entry.getKey(), entry.getValue());
            }
        } else {
            metaData = configMetadata;
        }
    }

    /**
     * 直接从 metaData 集合中获取配置值
     */
    @Override
    public Object getInternalProperty(String key) {
        return metaData.get(key);
    }

    public Map<String, String> getProperties() {
        return metaData;
    }
}
