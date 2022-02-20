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

package org.apache.shardingsphere.core.route.type.standard;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.api.hint.HintManager;
import org.apache.shardingsphere.core.exception.ShardingException;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.DeleteStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.UpdateStatement;
import org.apache.shardingsphere.core.route.router.sharding.condition.ShardingCondition;
import org.apache.shardingsphere.core.route.router.sharding.condition.ShardingConditions;
import org.apache.shardingsphere.core.route.type.RoutingEngine;
import org.apache.shardingsphere.core.route.type.RoutingResult;
import org.apache.shardingsphere.core.route.type.RoutingUnit;
import org.apache.shardingsphere.core.route.type.TableUnit;
import org.apache.shardingsphere.core.rule.BindingTableRule;
import org.apache.shardingsphere.core.rule.DataNode;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.TableRule;
import org.apache.shardingsphere.core.strategy.route.ShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.hint.HintShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.value.ListRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RouteValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * Standard routing engine.
 * 
 * @author zhangliang
 * @author maxiaoguang
 * @author panjuan
 *
 *  分片路由
 */
@RequiredArgsConstructor
public final class StandardRoutingEngine implements RoutingEngine {
    
    private final ShardingRule shardingRule;
    
    private final String logicTableName;
    
    private final SQLStatementContext sqlStatementContext;
    
    private final ShardingConditions shardingConditions;
    
    @Override
    public RoutingResult route() {
        if (isDMLForModify(sqlStatementContext.getSqlStatement()) && !sqlStatementContext.getTablesContext().isSingleTable()) {
            throw new ShardingException("Cannot support Multiple-Table for '%s'.", sqlStatementContext.getSqlStatement());
        }

        /**
         *  运行路由机制
         *   1、获取数据节点信息 {@link #getDataNodes(TableRule)}
         *   2、组装路由结果，并返回一个 RoutingResult {@link #generateRoutingResult(Collection)}
         */
        return generateRoutingResult(getDataNodes(shardingRule.getTableRule(logicTableName)));
    }
    
    private boolean isDMLForModify(final SQLStatement sqlStatement) {
        return sqlStatement instanceof InsertStatement || sqlStatement instanceof UpdateStatement || sqlStatement instanceof DeleteStatement;
    }
    
    private RoutingResult generateRoutingResult(final Collection<DataNode> routedDataNodes) {
        RoutingResult result = new RoutingResult();
        for (DataNode each : routedDataNodes) {

            // 根据每个 DataNode 构建一个 RoutingUnit 对象
            RoutingUnit routingUnit = new RoutingUnit(each.getDataSourceName());

            //填充 RoutingUnit 中的 TableUnit
            routingUnit.getTableUnits().add(new TableUnit(logicTableName, each.getTableName()));
            result.getRoutingUnits().add(routingUnit);
        }
        return result;
    }
    
    private Collection<DataNode> getDataNodes(final TableRule tableRule) {
        /**
         * 如基于Hint进行路由
         *  1、判断是否根据 Hint 来进行路由 {@link #isRoutingByHint(TableRule)}
         */
        if (isRoutingByHint(tableRule)) {
            return routeByHint(tableRule);
        }

        /**
         * 基于分片条件进行路由
         *  1、判断是否根据分片条件，进行路由 {@link #isRoutingByShardingConditions(TableRule)}
         */
        if (isRoutingByShardingConditions(tableRule)) {
            return routeByShardingConditions(tableRule);
        }

        /**
         * 执行混合路由 {@link #routeByMixedConditions(TableRule)}
         *
         *   - 无 ShardingCondition
         *      执行 {@link #routeByMixedConditionsWithHint(TableRule)}
         *   - 有 ShardingCondition
         *      执行 {@link #routeByMixedConditionsWithCondition(TableRule)}
         */
        return routeByMixedConditions(tableRule);
    }
    
    private boolean isRoutingByHint(final TableRule tableRule) {
        /**
         * 根据 DatabaseShardingStrategy 和 TableShardingStrategy 是否为 HintShardingStrategy
         */
        return shardingRule.getDatabaseShardingStrategy(tableRule) instanceof HintShardingStrategy && shardingRule.getTableShardingStrategy(tableRule) instanceof HintShardingStrategy;
    }
    
    private Collection<DataNode> routeByHint(final TableRule tableRule) {
        return route0(tableRule, getDatabaseShardingValuesFromHint(), getTableShardingValuesFromHint());
    }
    
    private boolean isRoutingByShardingConditions(final TableRule tableRule) {
        return !(shardingRule.getDatabaseShardingStrategy(tableRule) instanceof HintShardingStrategy || shardingRule.getTableShardingStrategy(tableRule) instanceof HintShardingStrategy);
    }
    
    private Collection<DataNode> routeByShardingConditions(final TableRule tableRule) {
        return shardingConditions.getConditions().isEmpty()
                ? route0(tableRule, Collections.<RouteValue>emptyList(), Collections.<RouteValue>emptyList()) : routeByShardingConditionsWithCondition(tableRule);
    }
    
    private Collection<DataNode> routeByShardingConditionsWithCondition(final TableRule tableRule) {
        Collection<DataNode> result = new LinkedList<>();
        for (ShardingCondition each : shardingConditions.getConditions()) {
            Collection<DataNode> dataNodes = route0(tableRule, getShardingValuesFromShardingConditions(shardingRule.getDatabaseShardingStrategy(tableRule).getShardingColumns(), each),
                    getShardingValuesFromShardingConditions(shardingRule.getTableShardingStrategy(tableRule).getShardingColumns(), each));
            each.getDataNodes().addAll(dataNodes);
            result.addAll(dataNodes);
        }
        return result;
    }
    
    private Collection<DataNode> routeByMixedConditions(final TableRule tableRule) {
        return shardingConditions.getConditions().isEmpty() ? routeByMixedConditionsWithHint(tableRule) : routeByMixedConditionsWithCondition(tableRule);
    }
    
    private Collection<DataNode> routeByMixedConditionsWithCondition(final TableRule tableRule) {
        Collection<DataNode> result = new LinkedList<>();
        for (ShardingCondition each : shardingConditions.getConditions()) {
            Collection<DataNode> dataNodes = route0(tableRule, getDatabaseShardingValues(tableRule, each), getTableShardingValues(tableRule, each));
            each.getDataNodes().addAll(dataNodes);
            result.addAll(dataNodes);
        }
        return result;
    }
    
    private Collection<DataNode> routeByMixedConditionsWithHint(final TableRule tableRule) {
        if (shardingRule.getDatabaseShardingStrategy(tableRule) instanceof HintShardingStrategy) {
            return route0(tableRule, getDatabaseShardingValuesFromHint(), Collections.<RouteValue>emptyList());
        }

        // 核心执行逻辑
        return route0(tableRule, Collections.<RouteValue>emptyList(), getTableShardingValuesFromHint());
    }
    
    private List<RouteValue> getDatabaseShardingValues(final TableRule tableRule, final ShardingCondition shardingCondition) {
        ShardingStrategy dataBaseShardingStrategy = shardingRule.getDatabaseShardingStrategy(tableRule);
        return isGettingShardingValuesFromHint(dataBaseShardingStrategy)
                ? getDatabaseShardingValuesFromHint() : getShardingValuesFromShardingConditions(dataBaseShardingStrategy.getShardingColumns(), shardingCondition);
    }
    
    private List<RouteValue> getTableShardingValues(final TableRule tableRule, final ShardingCondition shardingCondition) {
        ShardingStrategy tableShardingStrategy = shardingRule.getTableShardingStrategy(tableRule);
        return isGettingShardingValuesFromHint(tableShardingStrategy)
                ? getTableShardingValuesFromHint() : getShardingValuesFromShardingConditions(tableShardingStrategy.getShardingColumns(), shardingCondition);
    }
    
    private boolean isGettingShardingValuesFromHint(final ShardingStrategy shardingStrategy) {
        return shardingStrategy instanceof HintShardingStrategy;
    }
    
    private List<RouteValue> getDatabaseShardingValuesFromHint() {
        return getRouteValues(HintManager.getDatabaseShardingValues(logicTableName));
    }
    
    private List<RouteValue> getTableShardingValuesFromHint() {
        return getRouteValues(HintManager.getTableShardingValues(logicTableName));
    }
    
    private List<RouteValue> getRouteValues(final Collection<Comparable<?>> shardingValue) {
        return shardingValue.isEmpty() ? Collections.<RouteValue>emptyList() : Collections.<RouteValue>singletonList(new ListRouteValue<>("", logicTableName, shardingValue));
    }
    
    private List<RouteValue> getShardingValuesFromShardingConditions(final Collection<String> shardingColumns, final ShardingCondition shardingCondition) {
        List<RouteValue> result = new ArrayList<>(shardingColumns.size());
        for (RouteValue each : shardingCondition.getRouteValues()) {
            Optional<BindingTableRule> bindingTableRule = shardingRule.findBindingTableRule(logicTableName);
            if ((logicTableName.equals(each.getTableName()) || bindingTableRule.isPresent() && bindingTableRule.get().hasLogicTable(logicTableName)) 
                    && shardingColumns.contains(each.getColumnName())) {
                result.add(each);
            }
        }
        return result;
    }
    
    private Collection<DataNode> route0(final TableRule tableRule, final List<RouteValue> databaseShardingValues, final List<RouteValue> tableShardingValues) {

        //路由 DataSource
        Collection<String> routedDataSources = routeDataSources(tableRule, databaseShardingValues);
        Collection<DataNode> result = new LinkedList<>();

        // 路由Table，并完成 DataNode集合的拼装
        for (String each : routedDataSources) {
            result.addAll(routeTables(tableRule, each, tableShardingValues));
        }
        return result;
    }
    
    private Collection<String> routeDataSources(final TableRule tableRule, final List<RouteValue> databaseShardingValues) {
        if (databaseShardingValues.isEmpty()) {
            return tableRule.getActualDatasourceNames();
        }
        Collection<String> result = new LinkedHashSet<>(shardingRule.getDatabaseShardingStrategy(tableRule).doSharding(tableRule.getActualDatasourceNames(), databaseShardingValues));
        Preconditions.checkState(!result.isEmpty(), "no database route info");
        Preconditions.checkState(tableRule.getActualDatasourceNames().containsAll(result), 
                "Some routed data sources do not belong to configured data sources. routed data sources: `%s`, configured data sources: `%s`", result, tableRule.getActualDatasourceNames());
        return result;
    }
    
    private Collection<DataNode> routeTables(final TableRule tableRule, final String routedDataSource, final List<RouteValue> tableShardingValues) {
        Collection<String> availableTargetTables = tableRule.getActualTableNames(routedDataSource);
        Collection<String> routedTables = new LinkedHashSet<>(tableShardingValues.isEmpty() ? availableTargetTables
                : shardingRule.getTableShardingStrategy(tableRule).doSharding(availableTargetTables, tableShardingValues));
        Preconditions.checkState(!routedTables.isEmpty(), "no table route info");
        Collection<DataNode> result = new LinkedList<>();
        for (String each : routedTables) {
            result.add(new DataNode(routedDataSource, each));
        }
        return result;
    }
}
