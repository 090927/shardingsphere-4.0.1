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

package org.apache.shardingsphere.sql.parser.core.filler;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.shardingsphere.sql.parser.core.rule.registry.ParseRuleRegistry;
import org.apache.shardingsphere.sql.parser.core.rule.registry.statement.SQLStatementRule;
import org.apache.shardingsphere.sql.parser.sql.segment.SQLSegment;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;
import org.apache.shardingsphere.sql.parser.sql.statement.generic.AbstractSQLStatement;

import java.util.Collection;

/**
 * SQL statement filler engine.
 *
 * @author zhangliang
 * @author panjuan
 * @author duhongjun
 */
@RequiredArgsConstructor
public final class SQLStatementFillerEngine {
    
    private final ParseRuleRegistry parseRuleRegistry;
    
    private final String databaseTypeName;
    
    /**
     * Fill SQL statement.
     *
     * @param sqlSegments SQL segments
     * @param parameterMarkerCount parameter marker count
     * @param rule SQL statement rule
     * @return SQL statement
     *
     *  填充SQL 语句（ SQL 解析第三步）
     *    基于 `filler-rule-definition.xml` 配置文件。
     *       这里保存，SQLSegment 和 SQLSegmentFiller 之间的对应关系
     */
    @SuppressWarnings("unchecked")
    @SneakyThrows
    public SQLStatement fill(final Collection<SQLSegment> sqlSegments, final int parameterMarkerCount, final SQLStatementRule rule) {

        // 从 SQLStatementRule 中获取 SQLStatement 实例，如 CreateTableStatement
        SQLStatement result = rule.getSqlStatementClass().newInstance();

        // 通过断言对SQLStatement的合法性进行校验
        Preconditions.checkArgument(result instanceof AbstractSQLStatement, "%s must extends AbstractSQLStatement", result.getClass().getName());

        // 设置参数个数
        ((AbstractSQLStatement) result).setParametersCount(parameterMarkerCount);

        // 添加所有的 SQLSegment 到 SQLStatement 中
        result.getAllSQLSegments().addAll(sqlSegments);

        // 遍历填充对应类型的 SQLSegment
        for (SQLSegment each : sqlSegments) {

            /**
             * 根据数据库类型和 SQLSegment 找到对应 SQLSegmentFiller，并为 SQLStatement 填充 SQLSegment
             *   {@link ParseRuleRegistry#findSQLSegmentFiller(String, Class)}
             */
            Optional<SQLSegmentFiller> filler = parseRuleRegistry.findSQLSegmentFiller(databaseTypeName, each.getClass());
            if (filler.isPresent()) {
                /**
                 *  以 TableFiller 为例 {@link org.apache.shardingsphere.sql.parser.core.filler.impl.TableFiller
                 */
                filler.get().fill(each, result);
            }
        }
        return result;
    }
}
