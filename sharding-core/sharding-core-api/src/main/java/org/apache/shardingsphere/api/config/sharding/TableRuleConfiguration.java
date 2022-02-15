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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;
import org.apache.shardingsphere.api.config.sharding.strategy.ShardingStrategyConfiguration;

/**
 * Table rule configuration.
 * 
 * @author zhangliang
 * @author panjuan
 *
 *  表分片规则配置
 *
 *  对应 Yaml 配置体系 {@link org.apache.shardingsphere.core.yaml.config.sharding.YamlShardingRuleConfiguration }
 */
@Getter
@Setter
public final class TableRuleConfiguration {

    //逻辑表
    private final String logicTable;

    // 代表真实的数据节点，由数据源名+表名组成，支持行表达式。例如，"ds${0..1}.user${0..1}"就是比较典型的一种配置方式
    private final String actualDataNodes;

    // 代表分库策略，如果不设置，则使用默认分库策略，这里的默认分库策略就是 ShardingRuleConfiguration 中的 defaultDatabaseShardingStrategyConfig 配置
    private ShardingStrategyConfiguration databaseShardingStrategyConfig;

    // 代表分表策略。如果不设置，也会使用默认分表策略。
    private ShardingStrategyConfiguration tableShardingStrategyConfig;

    // 分布式环境，自增生成器配置，「 集成 雪花算法 」
    private KeyGeneratorConfiguration keyGeneratorConfig;
    
    public TableRuleConfiguration(final String logicTable) {
        this(logicTable, null);
    }
    
    public TableRuleConfiguration(final String logicTable, final String actualDataNodes) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(logicTable), "LogicTable is required.");
        this.logicTable = logicTable;
        this.actualDataNodes = actualDataNodes;
    }
}
