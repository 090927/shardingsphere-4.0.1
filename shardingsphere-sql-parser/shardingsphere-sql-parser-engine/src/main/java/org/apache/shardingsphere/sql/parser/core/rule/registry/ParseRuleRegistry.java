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

package org.apache.shardingsphere.sql.parser.core.rule.registry;

import com.google.common.base.Optional;
import org.apache.shardingsphere.spi.NewInstanceServiceLoader;
import org.apache.shardingsphere.sql.parser.core.filler.SQLSegmentFiller;
import org.apache.shardingsphere.sql.parser.core.rule.jaxb.entity.extractor.ExtractorRuleDefinitionEntity;
import org.apache.shardingsphere.sql.parser.core.rule.jaxb.entity.filler.FillerRuleDefinitionEntity;
import org.apache.shardingsphere.sql.parser.core.rule.jaxb.loader.RuleDefinitionFileConstant;
import org.apache.shardingsphere.sql.parser.core.rule.jaxb.loader.extractor.ExtractorRuleDefinitionEntityLoader;
import org.apache.shardingsphere.sql.parser.core.rule.jaxb.loader.filler.FillerRuleDefinitionEntityLoader;
import org.apache.shardingsphere.sql.parser.core.rule.jaxb.loader.statement.SQLStatementRuleDefinitionEntityLoader;
import org.apache.shardingsphere.sql.parser.core.rule.registry.extractor.ExtractorRuleDefinition;
import org.apache.shardingsphere.sql.parser.core.rule.registry.filler.FillerRuleDefinition;
import org.apache.shardingsphere.sql.parser.core.rule.registry.statement.SQLStatementRule;
import org.apache.shardingsphere.sql.parser.core.rule.registry.statement.SQLStatementRuleDefinition;
import org.apache.shardingsphere.sql.parser.spi.SQLParserEntry;
import org.apache.shardingsphere.sql.parser.sql.segment.SQLSegment;

import java.util.HashMap;
import java.util.Map;

/**
 * Parse rule registry.
 *
 * @author zhangliang
 * @author duhongjun
 */
public final class ParseRuleRegistry {
    
    private static volatile ParseRuleRegistry instance;

    /**
     * ExtractorRule 针对各个数据库差异，定义提取规则
     */
    private final ExtractorRuleDefinitionEntityLoader extractorRuleLoader = new ExtractorRuleDefinitionEntityLoader();

    /**
     * FillerRule 规则定义加载
     */
    private final FillerRuleDefinitionEntityLoader fillerRuleLoader = new FillerRuleDefinitionEntityLoader();

    /**
     * SQLStatementRule 规则定义加载
     */
    private final SQLStatementRuleDefinitionEntityLoader statementRuleLoader = new SQLStatementRuleDefinitionEntityLoader();

    /**
     * 存放，FillerRuleDefinition 过滤规则定义
     */
    private final Map<String, FillerRuleDefinition> fillerRuleDefinitions = new HashMap<>();

    /**
     * 存放，SQLStatementRuleDefinition 规则定义
     */
    private final Map<String, SQLStatementRuleDefinition> sqlStatementRuleDefinitions = new HashMap<>();
    
    static {

        // SPI 加载机制
        NewInstanceServiceLoader.register(SQLParserEntry.class);
        instance = new ParseRuleRegistry();
    }
    
    private ParseRuleRegistry() {
        initParseRuleDefinition();
    }

    /**
     * 初始化，规则定义加载
     *  1、extractor-rule-definition.xml  [ 提取SQLSegment]
     *  2、filler-rule-definition.xml   [ 填充SQL]
     *  3、sql-statement-rule-definition.xml  [解析语法树]
     */
    private void initParseRuleDefinition() {
        ExtractorRuleDefinitionEntity generalExtractorRuleEntity = extractorRuleLoader.load(RuleDefinitionFileConstant.getExtractorRuleDefinitionFile());
        FillerRuleDefinitionEntity generalFillerRuleEntity = fillerRuleLoader.load(RuleDefinitionFileConstant.getFillerRuleDefinitionFile());
        for (SQLParserEntry each : NewInstanceServiceLoader.newServiceInstances(SQLParserEntry.class)) {
            String databaseTypeName = each.getDatabaseTypeName();

            /**
             *  过滤规则定义加载 {@link #createFillerRuleDefinition(FillerRuleDefinitionEntity, String)}
             */
            fillerRuleDefinitions.put(databaseTypeName, createFillerRuleDefinition(generalFillerRuleEntity, databaseTypeName));

            /**
             * SQLStatementRule 规则定义加载 {@link #createSQLStatementRuleDefinition(ExtractorRuleDefinitionEntity, String)}
             */
            sqlStatementRuleDefinitions.put(databaseTypeName, createSQLStatementRuleDefinition(generalExtractorRuleEntity, databaseTypeName));
        }
    }

    private FillerRuleDefinition createFillerRuleDefinition(final FillerRuleDefinitionEntity generalFillerRuleEntity, final String databaseTypeName) {

        /**
         * 加载文件 `filler-rule-definition.xml`
         */
        FillerRuleDefinitionEntity databaseDialectFillerRuleEntity = fillerRuleLoader.load(RuleDefinitionFileConstant.getFillerRuleDefinitionFile(databaseTypeName));
        return new FillerRuleDefinition(generalFillerRuleEntity, databaseDialectFillerRuleEntity);
    }
    
    private SQLStatementRuleDefinition createSQLStatementRuleDefinition(final ExtractorRuleDefinitionEntity generalExtractorRuleEntity, final String databaseTypeName) {

        /**
         * 加载文件 `sql-statement-rule-definition.xml`
         *
         *   规则定义格式 [<sql-statement-rule context="select" sql-statement-class="org.apache.shardingsphere.sql.parser.sql.statement.dml.SelectStatement" extractor-rule-refs="tableReferences, columns, selectItems, where, predicate, groupBy, orderBy, limit, subqueryPredicate, lock" />]
         */
        ExtractorRuleDefinitionEntity databaseDialectExtractorRuleEntity = extractorRuleLoader.load(RuleDefinitionFileConstant.getExtractorRuleDefinitionFile(databaseTypeName));
        ExtractorRuleDefinition extractorRuleDefinition = new ExtractorRuleDefinition(generalExtractorRuleEntity, databaseDialectExtractorRuleEntity);
        return new SQLStatementRuleDefinition(statementRuleLoader.load(RuleDefinitionFileConstant.getSQLStatementRuleDefinitionFile(databaseTypeName)), extractorRuleDefinition);
    }
    
    /**
     * Get singleton instance of parsing rule registry.
     *
     * @return instance of parsing rule registry
     */
    public static ParseRuleRegistry getInstance() {
        return instance;
    }
    
    /**
     * Get SQL statement rule.
     *
     * @param databaseTypeName name of database type
     * @param contextClassName context class name
     * @return SQL statement rule
     */
    public SQLStatementRule getSQLStatementRule(final String databaseTypeName, final String contextClassName) {
        return sqlStatementRuleDefinitions.get(databaseTypeName).getSQLStatementRule(contextClassName);
    }
    
    /**
     * Find SQL segment rule.
     *
     * @param databaseTypeName name of database type
     * @param sqlSegmentClass SQL segment class
     * @return SQL segment rule
     */
    public Optional<SQLSegmentFiller> findSQLSegmentFiller(final String databaseTypeName, final Class<? extends SQLSegment> sqlSegmentClass) {
        return Optional.fromNullable(fillerRuleDefinitions.get(databaseTypeName).getFiller(sqlSegmentClass));
    }
}
