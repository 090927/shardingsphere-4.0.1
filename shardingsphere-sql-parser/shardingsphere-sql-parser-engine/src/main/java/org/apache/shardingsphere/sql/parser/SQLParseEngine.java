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

package org.apache.shardingsphere.sql.parser;

import com.google.common.base.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.shardingsphere.sql.parser.cache.SQLParseResultCache;
import org.apache.shardingsphere.sql.parser.core.SQLParseKernel;
import org.apache.shardingsphere.sql.parser.core.rule.registry.ParseRuleRegistry;
import org.apache.shardingsphere.sql.parser.hook.ParsingHook;
import org.apache.shardingsphere.sql.parser.hook.SPIParsingHook;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;

/**
 * SQL parse engine.
 *
 * @author zhangliang
 *
 *  SQL 解析
 */
@RequiredArgsConstructor
public final class SQLParseEngine {
    
    private final String databaseTypeName;
    
    private final SQLParseResultCache cache = new SQLParseResultCache();
    
    /**
     * Parse SQL.
     *
     * @param sql SQL
     * @param useCache use cache or not
     * @return SQL statement
     */
    public SQLStatement parse(final String sql, final boolean useCache) {

        /**
         * 基于 ‘Hook‘ 机制进行监控和跟踪 {@link SPIParsingHook
         */
        ParsingHook parsingHook = new SPIParsingHook();
        parsingHook.start(sql);
        try {

            /**
             * 完成 SQL 解析，并返回一个 SQLStatement {@link #parse0(String, boolean)}
             */
            SQLStatement result = parse0(sql, useCache);
            parsingHook.finishSuccess(result);
            return result;
            // CHECKSTYLE:OFF
        } catch (final Exception ex) {
            // CHECKSTYLE:ON
            parsingHook.finishFailure(ex);
            throw ex;
        }
    }
    
    private SQLStatement parse0(final String sql, final boolean useCache) {

        // 如果使用缓存，先尝试从缓存中，获取 SQLStatement
        if (useCache) {
            /**
             * 基于 guava 实现本地缓存 {@link SQLParseResultCache#getSQLStatement(String)}
             */
            Optional<SQLStatement> cachedSQLStatement = cache.getSQLStatement(sql);
            if (cachedSQLStatement.isPresent()) {
                return cachedSQLStatement.get();
            }
        }

        /**
         *  委托 SQLParseKernel 创建 SQLStatement {@link SQLParseKernel
         */
        SQLStatement result = new SQLParseKernel(ParseRuleRegistry.getInstance(), databaseTypeName, sql).parse();
        if (useCache) {
            cache.put(sql, result);
        }
        return result;
    }
}
