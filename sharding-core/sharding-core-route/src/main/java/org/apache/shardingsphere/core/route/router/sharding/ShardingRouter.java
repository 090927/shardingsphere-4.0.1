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

package org.apache.shardingsphere.core.route.router.sharding;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.api.hint.HintManager;
import org.apache.shardingsphere.core.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.core.metadata.table.TableMetas;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.core.route.router.sharding.condition.ShardingCondition;
import org.apache.shardingsphere.core.route.router.sharding.condition.ShardingConditions;
import org.apache.shardingsphere.core.route.router.sharding.condition.engine.InsertClauseShardingConditionEngine;
import org.apache.shardingsphere.core.route.router.sharding.condition.engine.WhereClauseShardingConditionEngine;
import org.apache.shardingsphere.core.route.router.sharding.keygen.GeneratedKey;
import org.apache.shardingsphere.core.route.router.sharding.validator.ShardingStatementValidator;
import org.apache.shardingsphere.core.route.router.sharding.validator.ShardingStatementValidatorFactory;
import org.apache.shardingsphere.core.route.type.RoutingEngine;
import org.apache.shardingsphere.core.route.type.RoutingResult;
import org.apache.shardingsphere.core.rule.BindingTableRule;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.core.rule.TableRule;
import org.apache.shardingsphere.core.strategy.route.hint.HintShardingStrategy;
import org.apache.shardingsphere.core.strategy.route.value.ListRouteValue;
import org.apache.shardingsphere.core.strategy.route.value.RouteValue;
import org.apache.shardingsphere.sql.parser.SQLParseEngine;
import org.apache.shardingsphere.sql.parser.relation.SQLStatementContextFactory;
import org.apache.shardingsphere.sql.parser.relation.metadata.RelationMetas;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.impl.InsertSQLStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.impl.SelectSQLStatementContext;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.DMLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.InsertStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.dml.UpdateStatement;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Sharding router.
 *
 * @author zhangliang
 * @author maxiaoguang
 * @author panjuan
 * @author zhangyonglun
 */
@RequiredArgsConstructor
public final class ShardingRouter {

    // 基础配置类，代表分片的各种规则信息
    private final ShardingRule shardingRule;
    
    private final ShardingSphereMetaData metaData;

    // SQL 解析引擎
    private final SQLParseEngine parseEngine;
    
    private final List<Comparable<?>> generatedValues = new LinkedList<>();
    
    /**
     * Parse SQL.
     * To make sure SkyWalking will be available at the next release of ShardingSphere,
     * a new plugin should be provided to SkyWalking project if this API changed.
     *
     * @see <a href="https://github.com/apache/skywalking/blob/master/docs/en/guides/Java-Plugin-Development-Guide.md#user-content-plugin-development-guide">Plugin Development Guide</a>
     *
     * @param logicSQL logic SQL
     * @param useCache use cache to save SQL parse result or not
     * @return parse result
     *
     *  logicSQL 命名，以便区别在，分片和读写分离情况下的真实SQL。
     */
    public SQLStatement parse(final String logicSQL, final boolean useCache) {
        /**
         * SQL 解析,返回一个 SQLStatement 对象 {@link SQLParseEngine#parse(String, boolean)}
         */
        return parseEngine.parse(logicSQL, useCache);
    }
    
    /**
     * Route SQL.
     *
     * @param logicSQL logic SQL
     * @param parameters SQL parameters
     * @param sqlStatement SQL statement
     * @return parse result
     */
    @SuppressWarnings("unchecked")
    public SQLRouteResult route(final String logicSQL, final List<Object> parameters, final SQLStatement sqlStatement) {

        /**
         * 分片合理性验证 {@link ShardingStatementValidatorFactory#newInstance(SQLStatement)}
         */
        Optional<ShardingStatementValidator> shardingStatementValidator = ShardingStatementValidatorFactory.newInstance(sqlStatement);
        if (shardingStatementValidator.isPresent()) {

            /**
             *  1、验证分片合理性
             *      {@link org.apache.shardingsphere.core.route.router.sharding.validator.impl.ShardingInsertStatementValidator#validate(ShardingRule, InsertStatement, List)}
             *      {@link org.apache.shardingsphere.core.route.router.sharding.validator.impl.ShardingUpdateStatementValidator#validate(ShardingRule, UpdateStatement, List)}
             */
            shardingStatementValidator.get().validate(shardingRule, sqlStatement, parameters);
        }

        /**
         * 2、获取上下文 {@link SQLStatementContextFactory#newInstance(RelationMetas, String, List, SQLStatement)}
         */
        SQLStatementContext sqlStatementContext = SQLStatementContextFactory.newInstance(metaData.getRelationMetas(), logicSQL, parameters, sqlStatement);

        /**
         * 3、主键生成方案 {@link GeneratedKey#getGenerateKey(ShardingRule, TableMetas, List, InsertStatement)}
         */
        Optional<GeneratedKey> generatedKey = sqlStatement instanceof InsertStatement
                ? GeneratedKey.getGenerateKey(shardingRule, metaData.getTables(), parameters, (InsertStatement) sqlStatement) : Optional.<GeneratedKey>absent();

        /**
         * 4、创建分片条件 {@link #getShardingConditions(List, SQLStatementContext, GeneratedKey, RelationMetas)}
         */
        ShardingConditions shardingConditions = getShardingConditions(parameters, sqlStatementContext, generatedKey.orNull(), metaData.getRelationMetas());
        boolean needMergeShardingValues = isNeedMergeShardingValues(sqlStatementContext);
        if (sqlStatementContext.getSqlStatement() instanceof DMLStatement && needMergeShardingValues) {
            checkSubqueryShardingValues(sqlStatementContext, shardingConditions);
            mergeShardingConditions(shardingConditions);
        }

        /**
         * 5、基于 SQLStatement 生成具体 RoutingEngine  {@link RoutingEngineFactory#newInstance(ShardingRule, ShardingSphereMetaData, SQLStatementContext, ShardingConditions)}
         */
        RoutingEngine routingEngine = RoutingEngineFactory.newInstance(shardingRule, metaData, sqlStatementContext, shardingConditions);

        // 执行结果
        RoutingResult routingResult = routingEngine.route();
        if (needMergeShardingValues) {
            Preconditions.checkState(1 == routingResult.getRoutingUnits().size(), "Must have one sharding with subquery.");
        }

        /**
         * 6、执行路由结果
         */
        SQLRouteResult result = new SQLRouteResult(sqlStatementContext, shardingConditions, generatedKey.orNull());
        result.setRoutingResult(routingResult);

        // 如果是 Insert 语句，则设置自动生成分片键
        if (sqlStatementContext instanceof InsertSQLStatementContext) {
            setGeneratedValues(result);
        }
        return result;
    }

    /**
     * 创建分片条件
     */
    private ShardingConditions getShardingConditions(final List<Object> parameters, final SQLStatementContext sqlStatementContext, final GeneratedKey generatedKey, final RelationMetas relationMetas) {

        // 根据，输入的SQL 类型。创建不同的分片条件。
        if (sqlStatementContext.getSqlStatement() instanceof DMLStatement) {

            // 如果是 InsertSQLStatement 上下文
            if (sqlStatementContext instanceof InsertSQLStatementContext) {
                InsertSQLStatementContext shardingInsertStatement = (InsertSQLStatementContext) sqlStatementContext;

                //通过 InsertClauseShardingConditionEngine 创建分片条件
                return new ShardingConditions(new InsertClauseShardingConditionEngine(shardingRule).createShardingConditions(shardingInsertStatement, generatedKey, parameters));
            }

            // 否则直接通过 WhereClauseShardingConditionEngine 创建分片条件
            return new ShardingConditions(new WhereClauseShardingConditionEngine(shardingRule, relationMetas).createShardingConditions(sqlStatementContext.getSqlStatement(), parameters));
        }
        return new ShardingConditions(Collections.<ShardingCondition>emptyList());
    }
    
    private boolean isNeedMergeShardingValues(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof SelectSQLStatementContext && ((SelectSQLStatementContext) sqlStatementContext).isContainsSubquery() 
                && !shardingRule.getShardingLogicTableNames(sqlStatementContext.getTablesContext().getTableNames()).isEmpty();
    }
    
    private void checkSubqueryShardingValues(final SQLStatementContext sqlStatementContext, final ShardingConditions shardingConditions) {
        for (String each : sqlStatementContext.getTablesContext().getTableNames()) {
            Optional<TableRule> tableRule = shardingRule.findTableRule(each);
            if (tableRule.isPresent() && isRoutingByHint(tableRule.get()) && !HintManager.getDatabaseShardingValues(each).isEmpty() && !HintManager.getTableShardingValues(each).isEmpty()) {
                return;
            }
        }
        Preconditions.checkState(!shardingConditions.getConditions().isEmpty(), "Must have sharding column with subquery.");
        if (shardingConditions.getConditions().size() > 1) {
            Preconditions.checkState(isSameShardingCondition(shardingConditions), "Sharding value must same with subquery.");
        }
    }
    
    private boolean isRoutingByHint(final TableRule tableRule) {
        return shardingRule.getDatabaseShardingStrategy(tableRule) instanceof HintShardingStrategy && shardingRule.getTableShardingStrategy(tableRule) instanceof HintShardingStrategy;
    }
    
    private boolean isSameShardingCondition(final ShardingConditions shardingConditions) {
        ShardingCondition example = shardingConditions.getConditions().remove(shardingConditions.getConditions().size() - 1);
        for (ShardingCondition each : shardingConditions.getConditions()) {
            if (!isSameShardingCondition(example, each)) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isSameShardingCondition(final ShardingCondition shardingCondition1, final ShardingCondition shardingCondition2) {
        if (shardingCondition1.getRouteValues().size() != shardingCondition2.getRouteValues().size()) {
            return false;
        }
        for (int i = 0; i < shardingCondition1.getRouteValues().size(); i++) {
            RouteValue shardingValue1 = shardingCondition1.getRouteValues().get(i);
            RouteValue shardingValue2 = shardingCondition2.getRouteValues().get(i);
            if (!isSameRouteValue((ListRouteValue) shardingValue1, (ListRouteValue) shardingValue2)) {
                return false;
            }
        }
        return true;
    }
    
    private boolean isSameRouteValue(final ListRouteValue routeValue1, final ListRouteValue routeValue2) {
        return isSameLogicTable(routeValue1, routeValue2)
                && routeValue1.getColumnName().equals(routeValue2.getColumnName()) && routeValue1.getValues().equals(routeValue2.getValues());
    }
    
    private boolean isSameLogicTable(final ListRouteValue shardingValue1, final ListRouteValue shardingValue2) {
        return shardingValue1.getTableName().equals(shardingValue2.getTableName()) || isBindingTable(shardingValue1, shardingValue2);
    }
    
    private boolean isBindingTable(final ListRouteValue shardingValue1, final ListRouteValue shardingValue2) {
        Optional<BindingTableRule> bindingRule = shardingRule.findBindingTableRule(shardingValue1.getTableName());
        return bindingRule.isPresent() && bindingRule.get().hasLogicTable(shardingValue2.getTableName());
    }
    
    private void mergeShardingConditions(final ShardingConditions shardingConditions) {
        if (shardingConditions.getConditions().size() > 1) {
            ShardingCondition shardingCondition = shardingConditions.getConditions().remove(shardingConditions.getConditions().size() - 1);
            shardingConditions.getConditions().clear();
            shardingConditions.getConditions().add(shardingCondition);
        }
    }
    
    private void setGeneratedValues(final SQLRouteResult sqlRouteResult) {
        if (sqlRouteResult.getGeneratedKey().isPresent()) {
            generatedValues.addAll(sqlRouteResult.getGeneratedKey().get().getGeneratedValues());
            sqlRouteResult.getGeneratedKey().get().getGeneratedValues().clear();
            sqlRouteResult.getGeneratedKey().get().getGeneratedValues().addAll(generatedValues);
        }
    }
}
