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

package org.apache.shardingsphere.core;

import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.api.hint.HintManager;
import org.apache.shardingsphere.core.constant.properties.ShardingProperties;
import org.apache.shardingsphere.core.constant.properties.ShardingPropertiesConstant;
import org.apache.shardingsphere.core.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.core.route.RouteUnit;
import org.apache.shardingsphere.core.route.SQLLogger;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.core.route.SQLUnit;
import org.apache.shardingsphere.core.route.hook.SPIRoutingHook;
import org.apache.shardingsphere.core.route.type.RoutingUnit;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.encrypt.rewrite.context.EncryptSQLRewriteContextDecorator;
import org.apache.shardingsphere.sharding.rewrite.context.ShardingSQLRewriteContextDecorator;
import org.apache.shardingsphere.sharding.rewrite.engine.ShardingSQLRewriteEngine;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;
import org.apache.shardingsphere.underlying.rewrite.context.SQLRewriteContext;
import org.apache.shardingsphere.underlying.rewrite.engine.SQLRewriteResult;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Base sharding engine.
 *
 * @author zhangliang
 * @author panjuan
 */
@RequiredArgsConstructor
public abstract class BaseShardingEngine {
    
    private final ShardingRule shardingRule;
    
    private final ShardingProperties shardingProperties;
    
    private final ShardingSphereMetaData metaData;
    
    private final SPIRoutingHook routingHook = new SPIRoutingHook();
    
    /**
     * Shard.
     *
     * @param sql SQL
     * @param parameters parameters of SQL
     * @return SQL route result
     */
    public SQLRouteResult shard(final String sql, final List<Object> parameters) {

        //调用模板方法准备参数
        List<Object> clonedParameters = cloneParameters(parameters);

        /**
         *  执行路由 {@link #executeRoute(String, List)}
         */
        SQLRouteResult result = executeRoute(sql, clonedParameters);

        /**
         * 执行 SQL 转换（Convert）和改写（Rewrite）
         *
         *  1、改写逻辑 {@link #rewriteAndConvert(String, List, SQLRouteResult)}
         */
        result.getRouteUnits().addAll(HintManager.isDatabaseShardingOnly() ? convert(sql, clonedParameters, result) : rewriteAndConvert(sql, clonedParameters, result));
        boolean showSQL = shardingProperties.getValue(ShardingPropertiesConstant.SQL_SHOW);
        if (showSQL) {
            boolean showSimple = shardingProperties.getValue(ShardingPropertiesConstant.SQL_SIMPLE);
            SQLLogger.logSQL(sql, showSimple, result.getSqlStatementContext(), result.getRouteUnits());
        }
        return result;
    }

    // 模板方法- 拷贝参数
    protected abstract List<Object> cloneParameters(List<Object> parameters);

    // 模板方法- 执行路由
    protected abstract SQLRouteResult route(String sql, List<Object> parameters);
    
    private SQLRouteResult executeRoute(final String sql, final List<Object> clonedParameters) {
        routingHook.start(sql);
        try {

            /**
             *  [ShardingRoute 实现] {@link org.apache.shardingsphere.core.route.router.sharding.ShardingRouter#route(String, List, SQLStatement)}
             */
            SQLRouteResult result = route(sql, clonedParameters);
            routingHook.finishSuccess(result, metaData.getTables());
            return result;
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            routingHook.finishFailure(ex);
            throw ex;
        }
    }
    
    private Collection<RouteUnit> convert(final String sql, final List<Object> parameters, final SQLRouteResult sqlRouteResult) {
        Collection<RouteUnit> result = new LinkedHashSet<>();
        for (RoutingUnit each : sqlRouteResult.getRoutingResult().getRoutingUnits()) {
            result.add(new RouteUnit(each.getDataSourceName(), new SQLUnit(sql, parameters)));
        }
        return result;
    }

    /**
     * 改写 SQL 语句
     */
    private Collection<RouteUnit> rewriteAndConvert(final String sql, final List<Object> parameters, final SQLRouteResult sqlRouteResult) {
        /**
         * 构建 SQLRewriteContext {@link SQLRewriteContext
         */
        SQLRewriteContext sqlRewriteContext = new SQLRewriteContext(metaData.getRelationMetas(), sqlRouteResult.getSqlStatementContext(), sql, parameters);

        /**
         * 对 SQLRewriteContext 进行装饰 {@link ShardingSQLRewriteContextDecorator#decorate(SQLRewriteContext)}
         */
        new ShardingSQLRewriteContextDecorator(shardingRule, sqlRouteResult).decorate(sqlRewriteContext);

        // 判断是否根据数据脱敏列进行查询
        boolean isQueryWithCipherColumn = shardingProperties.<Boolean>getValue(ShardingPropertiesConstant.QUERY_WITH_CIPHER_COLUMN);

        //构建 EncryptSQLRewriteContextDecorator 对 SQLRewriteContext 进行装饰
        new EncryptSQLRewriteContextDecorator(shardingRule.getEncryptRule(), isQueryWithCipherColumn).decorate(sqlRewriteContext);

        /**
         * 生成 SQLTokens {@link SQLRewriteContext#generateSQLTokens()}
         */
        sqlRewriteContext.generateSQLTokens();
        Collection<RouteUnit> result = new LinkedHashSet<>();
        for (RoutingUnit each : sqlRouteResult.getRoutingResult().getRoutingUnits()) {

            //构建 ShardingSQLRewriteEngine
            ShardingSQLRewriteEngine sqlRewriteEngine = new ShardingSQLRewriteEngine(shardingRule, sqlRouteResult.getShardingConditions(), each);

            /**
             *  SQL 改写 {@link ShardingSQLRewriteEngine#rewrite(SQLRewriteContext)}
             *   例如: 表名改写，逻辑表名 -> 真实表名
             */
            SQLRewriteResult sqlRewriteResult = sqlRewriteEngine.rewrite(sqlRewriteContext);

            //保存改写结果
            result.add(new RouteUnit(each.getDataSourceName(), new SQLUnit(sqlRewriteResult.getSql(), sqlRewriteResult.getParameters())));
        }
        return result;
    }
}
