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

package org.apache.shardingsphere.sql.parser.core.extractor;

import com.google.common.base.Optional;
import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.shardingsphere.sql.parser.core.extractor.api.CollectionSQLSegmentExtractor;
import org.apache.shardingsphere.sql.parser.core.extractor.api.OptionalSQLSegmentExtractor;
import org.apache.shardingsphere.sql.parser.core.extractor.api.SQLSegmentExtractor;
import org.apache.shardingsphere.sql.parser.core.parser.SQLAST;
import org.apache.shardingsphere.sql.parser.sql.segment.SQLSegment;

import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

/**
 * SQL segments extractor engine.
 * 
 * @author zhangliang
 */
public final class SQLSegmentsExtractorEngine {
    
    /** 
     * Extract SQL segments.
     * 
     * @param ast SQL AST
     * @return SQL segments
     *
     *  ShardingSphere 提供各种数据库的 SQLSegment 的提取定义。
     *      以 MySQL 为例，代码工厂 META-INF/parsing-rule-definition/mysql 目录 `extractor-rule-definition.xml`
     *
     *      用来提取 SQLAST 语法树中的SQL片段
     */
    public Collection<SQLSegment> extract(final SQLAST ast) {
        Collection<SQLSegment> result = new LinkedList<>();

        // 遍历提取器，从 Context 中提取对应类型的 SQLSegment，比如 TableSegment
        for (SQLSegmentExtractor each : ast.getSqlStatementRule().getExtractors()) {

            // 单节点的场景，直接提取单一节点下的内容
            if (each instanceof OptionalSQLSegmentExtractor) {
                Optional<? extends SQLSegment> sqlSegment = ((OptionalSQLSegmentExtractor) each).extract(ast.getParserRuleContext(), ast.getParameterMarkerIndexes());
                if (sqlSegment.isPresent()) {
                    result.add(sqlSegment.get());
                }
            } else if (each instanceof CollectionSQLSegmentExtractor) {
                /**
                 * 提取`SQLSegment` 、以 Table 为例 {@link org.apache.shardingsphere.sql.parser.core.extractor.impl.common.table.TableExtractor#extract(ParserRuleContext, Map)}
                 */
                result.addAll(((CollectionSQLSegmentExtractor) each).extract(ast.getParserRuleContext(), ast.getParameterMarkerIndexes()));
            }
        }
        return result;
    }
}
