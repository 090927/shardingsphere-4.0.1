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

package org.apache.shardingsphere.sql.parser.core;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.shardingsphere.sql.parser.core.extractor.SQLSegmentsExtractorEngine;
import org.apache.shardingsphere.sql.parser.core.filler.SQLStatementFillerEngine;
import org.apache.shardingsphere.sql.parser.core.parser.SQLAST;
import org.apache.shardingsphere.sql.parser.core.parser.SQLParserEngine;
import org.apache.shardingsphere.sql.parser.core.rule.registry.ParseRuleRegistry;
import org.apache.shardingsphere.sql.parser.core.rule.registry.statement.SQLStatementRule;
import org.apache.shardingsphere.sql.parser.sql.segment.SQLSegment;
import org.apache.shardingsphere.sql.parser.sql.statement.SQLStatement;

import java.util.Collection;
import java.util.Map;

/**
 * SQL parse kernel.
 *
 * @author duhongjun
 * @author zhangliang
 *
 *  sql
 */
public final class SQLParseKernel {

    // SQL解析器引擎
    private final SQLParserEngine parserEngine;

    // SQLSegment 提取器引擎
    private final SQLSegmentsExtractorEngine extractorEngine;

    // SQLStatement 填充器引擎
    private final SQLStatementFillerEngine fillerEngine;
    
    public SQLParseKernel(final ParseRuleRegistry parseRuleRegistry, final String databaseTypeName, final String sql) {
        parserEngine = new SQLParserEngine(parseRuleRegistry, databaseTypeName, sql);
        extractorEngine = new SQLSegmentsExtractorEngine();
        fillerEngine = new SQLStatementFillerEngine(parseRuleRegistry, databaseTypeName);
    }
    
    /**
     * Parse SQL.
     *
     * @return SQL statement
     */
    public SQLStatement parse() {

        /**
         * 利用 ANTLR4 解析 SQL 抽象语法树 {@link SQLParserEngine#parse()}
         */
        SQLAST ast = parserEngine.parse();

        /**
         * 提取 AST 中 Token，封装对应 TableSegment、IndexSegment {@link SQLSegmentsExtractorEngine#extract(SQLAST)}
         */
        Collection<SQLSegment> sqlSegments = extractorEngine.extract(ast);
        Map<ParserRuleContext, Integer> parameterMarkerIndexes = ast.getParameterMarkerIndexes();

        /**
         * 填充 SQLStatement 并返回 {@link SQLStatementFillerEngine#fill(Collection, int, SQLStatementRule)}
         */
        return fillerEngine.fill(sqlSegments, parameterMarkerIndexes.size(), ast.getSqlStatementRule());
    }
}
