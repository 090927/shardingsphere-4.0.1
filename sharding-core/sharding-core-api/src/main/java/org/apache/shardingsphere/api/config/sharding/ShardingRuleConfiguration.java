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

package org.apache.shardingsphere.api.config.sharding;

import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.api.config.RuleConfiguration;
import org.apache.shardingsphere.api.config.encrypt.EncryptRuleConfiguration;
import org.apache.shardingsphere.api.config.masterslave.MasterSlaveRuleConfiguration;
import org.apache.shardingsphere.api.config.sharding.strategy.ShardingStrategyConfiguration;

import java.util.Collection;
import java.util.LinkedList;

/**
 * Sharding rule configuration.
 * 
 * @author zhangliang
 * @author maxiaoguang
 * @author panjuan
 *
 * 用于分片规则的配置入口。
 */
@Getter
@Setter
public final class ShardingRuleConfiguration implements RuleConfiguration {

    // 表分片规则列表
    private Collection<TableRuleConfiguration> tableRuleConfigs = new LinkedList<>();

    // 绑定表规则列表
    private Collection<String> bindingTableGroups = new LinkedList<>();

    // 广播表规则列表
    private Collection<String> broadcastTables = new LinkedList<>();

    // 默认数据源
    private String defaultDataSourceName;

    // 默认分库策略
    private ShardingStrategyConfiguration defaultDatabaseShardingStrategyConfig;

    // 默认分表策略
    private ShardingStrategyConfiguration defaultTableShardingStrategyConfig;

    // 默认自增列值生成器
    private KeyGeneratorConfiguration defaultKeyGeneratorConfig;

    /**
     * 读写分离规则 {@link MasterSlaveRuleConfiguration }
     */
    private Collection<MasterSlaveRuleConfiguration> masterSlaveRuleConfigs = new LinkedList<>();

    //数据脱敏规则
    private EncryptRuleConfiguration encryptRuleConfig;
}
