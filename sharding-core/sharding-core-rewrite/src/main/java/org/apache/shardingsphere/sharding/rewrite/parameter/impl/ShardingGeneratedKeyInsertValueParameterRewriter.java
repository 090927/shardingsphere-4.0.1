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

package org.apache.shardingsphere.sharding.rewrite.parameter.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import lombok.Setter;
import org.apache.shardingsphere.core.route.SQLRouteResult;
import org.apache.shardingsphere.sql.parser.relation.statement.SQLStatementContext;
import org.apache.shardingsphere.sql.parser.relation.statement.impl.InsertSQLStatementContext;
import org.apache.shardingsphere.sharding.rewrite.aware.SQLRouteResultAware;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.ParameterBuilder;
import org.apache.shardingsphere.underlying.rewrite.parameter.builder.impl.GroupedParameterBuilder;
import org.apache.shardingsphere.underlying.rewrite.parameter.rewriter.ParameterRewriter;

import java.util.Iterator;
import java.util.List;

/**
 * Sharding generated key insert value parameter rewriter.
 *
 * @author zhangliang
 */
@Setter
public final class ShardingGeneratedKeyInsertValueParameterRewriter implements ParameterRewriter, SQLRouteResultAware {
    
    private SQLRouteResult sqlRouteResult;
    
    @Override
    public boolean isNeedRewrite(final SQLStatementContext sqlStatementContext) {
        return sqlStatementContext instanceof InsertSQLStatementContext && sqlRouteResult.getGeneratedKey().isPresent() && sqlRouteResult.getGeneratedKey().get().isGenerated();
    }

    /**
     *  因为 `ParameterRewriter` 面向 `InsertSQLStatement` 所以用到 GroupParameterBuilder.
     */
    @Override
    public void rewrite(final ParameterBuilder parameterBuilder, final SQLStatementContext sqlStatementContext, final List<Object> parameters) {
        Preconditions.checkState(sqlRouteResult.getGeneratedKey().isPresent());
        ((GroupedParameterBuilder) parameterBuilder).setDerivedColumnName(sqlRouteResult.getGeneratedKey().get().getColumnName());

        // 获取 GeneratedKey
        Iterator<Comparable<?>> generatedValues = sqlRouteResult.getGeneratedKey().get().getGeneratedValues().descendingIterator();
        int count = 0;
        int parametersCount = 0;
        for (List<Object> each : ((InsertSQLStatementContext) sqlStatementContext).getGroupedParameters()) {
            parametersCount += ((InsertSQLStatementContext) sqlStatementContext).getInsertValueContexts().get(count).getParametersCount();
            Comparable<?> generatedValue = generatedValues.next();
            if (!each.isEmpty()) {

                //使用 GroupedParameterBuilder 进行补列和设置参数
                ((GroupedParameterBuilder) parameterBuilder).getParameterBuilders().get(count).addAddedParameters(parametersCount, Lists.<Object>newArrayList(generatedValue));
            }
            count++;
        }
    }
}
