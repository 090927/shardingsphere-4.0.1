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

package org.apache.shardingsphere.sharding.merge.dql.groupby;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.shardingsphere.sharding.merge.dql.groupby.aggregation.*;
import org.apache.shardingsphere.sharding.merge.dql.orderby.OrderByStreamMergedResult;
import org.apache.shardingsphere.sql.parser.core.constant.AggregationType;
import org.apache.shardingsphere.sql.parser.relation.segment.select.projection.impl.AggregationDistinctProjection;
import org.apache.shardingsphere.sql.parser.relation.segment.select.projection.impl.AggregationProjection;
import org.apache.shardingsphere.sql.parser.relation.statement.impl.SelectSQLStatementContext;
import org.apache.shardingsphere.underlying.execute.QueryResult;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Stream merged result for group by.
 *
 * @author zhangliang
 *
 *  分组归并
 */
public final class GroupByStreamMergedResult extends OrderByStreamMergedResult {
    
    private final SelectSQLStatementContext selectSQLStatementContext;
    
    private final List<Object> currentRow;
    
    private List<?> currentGroupByValues;
    
    public GroupByStreamMergedResult(
            final Map<String, Integer> labelAndIndexMap, final List<QueryResult> queryResults, final SelectSQLStatementContext selectSQLStatementContext) throws SQLException {
        super(queryResults, selectSQLStatementContext.getOrderByContext().getItems());
        this.selectSQLStatementContext = selectSQLStatementContext;
        currentRow = new ArrayList<>(labelAndIndexMap.size());

        // 如果优先级队列不为空，就将队列中第一个元素的分组值赋值给 currentGroupByValues 变量
        currentGroupByValues = getOrderByValuesQueue().isEmpty()
                ? Collections.emptyList() : new GroupByValue(getCurrentQueryResult(), selectSQLStatementContext.getGroupByContext().getItems()).getGroupValues();
    }
    
    @Override
    public boolean next() throws SQLException {

        // 清除当前结果记录
        currentRow.clear();
        if (getOrderByValuesQueue().isEmpty()) {
            return false;
        }
        if (isFirstNext()) {
            super.next();
        }

        // 顺序合并相同分组条件的记录
        if (aggregateCurrentGroupByRowAndNext()) {

            // 生成下一条结果记录分组值
            currentGroupByValues = new GroupByValue(getCurrentQueryResult(), selectSQLStatementContext.getGroupByContext().getItems()).getGroupValues();
        }
        return true;
    }
    
    private boolean aggregateCurrentGroupByRowAndNext() throws SQLException {
        boolean result = false;

        // 生成计算单元
        Map<AggregationProjection, AggregationUnit> aggregationUnitMap = Maps.toMap(

                // 通过 selectSQLStatementContext 获取 select 语句所有聚合类型的项
                selectSQLStatementContext.getProjectionsContext().getAggregationProjections(), new Function<AggregationProjection, AggregationUnit>() {
                    
                    @Override
                    // 通过工厂方法获取具体的聚合单元
                    public AggregationUnit apply(final AggregationProjection input) {

                        /**
                         * 聚合单元对象工厂类 {@link AggregationUnitFactory#create(AggregationType, boolean)}
                         */
                        return AggregationUnitFactory.create(input.getType(), input instanceof AggregationDistinctProjection);
                    }
                });

        // 循环顺序合并相同分组条件的记录
        while (currentGroupByValues.equals(new GroupByValue(getCurrentQueryResult(), selectSQLStatementContext.getGroupByContext().getItems()).getGroupValues())) {

            /**
             * 计算聚合值 {@link #aggregate(Map)}
             */
            aggregate(aggregationUnitMap);
            cacheCurrentRow();

            // 获取下一条记录，调用父类中的next方法从而使得currentResultSet指向下一个元素
            result = super.next();

            // 如果值已经遍历完毕，则结束循环
            if (!result) {
                break;
            }
        }

        /**
         * 设置当前记录的聚合字段结果 {@link #setAggregationValueToCurrentRow(Map)}
         */
        setAggregationValueToCurrentRow(aggregationUnitMap);
        return result;
    }
    
    private void aggregate(final Map<AggregationProjection, AggregationUnit> aggregationUnitMap) throws SQLException {
        for (Entry<AggregationProjection, AggregationUnit> entry : aggregationUnitMap.entrySet()) {
            List<Comparable<?>> values = new ArrayList<>(2);
            if (entry.getKey().getDerivedAggregationProjections().isEmpty()) {
                values.add(getAggregationValue(entry.getKey()));
            } else {
                for (AggregationProjection each : entry.getKey().getDerivedAggregationProjections()) {
                    values.add(getAggregationValue(each));
                }
            }

            /**
             * 计算聚合值 {@link AggregationUnit#merge(List)}
             *      实现类
             *        [max] {@link ComparableAggregationUnit#merge(List)}
             *        [min] {@link ComparableAggregationUnit#merge(List)}
             *        [sum] {@link DistinctSumAggregationUnit#merge(List)}
             *        [count] {@link DistinctCountAggregationUnit#merge(List)}
             *        [avg] {@link DistinctAverageAggregationUnit#merge(List)}
             */
            entry.getValue().merge(values);
        }
    }
    
    private void cacheCurrentRow() throws SQLException {
        for (int i = 0; i < getCurrentQueryResult().getColumnCount(); i++) {
            currentRow.add(getCurrentQueryResult().getValue(i + 1, Object.class));
        }
    }
    
    private Comparable<?> getAggregationValue(final AggregationProjection aggregationSelectItem) throws SQLException {
        Object result = getCurrentQueryResult().getValue(aggregationSelectItem.getIndex(), Object.class);
        Preconditions.checkState(null == result || result instanceof Comparable, "Aggregation value must implements Comparable");
        return (Comparable<?>) result;
    }
    
    private void setAggregationValueToCurrentRow(final Map<AggregationProjection, AggregationUnit> aggregationUnitMap) {
        for (Entry<AggregationProjection, AggregationUnit> entry : aggregationUnitMap.entrySet()) {
            /**
             *  [result] {@link AggregationUnit#getResult()}
             *      实现类
             *        [max] {@link ComparableAggregationUnit#getResult()}
             *        [min] {@link ComparableAggregationUnit#getResult()}
             *        [sum] {@link DistinctSumAggregationUnit#getResult()}
             *        [count] {@link DistinctCountAggregationUnit#getResult()}
             *        [avg] {@link DistinctAverageAggregationUnit#getResult()}
             *
             */
            currentRow.set(entry.getKey().getIndex() - 1, entry.getValue().getResult());
        }
    }
    
    @Override
    public Object getValue(final int columnIndex, final Class<?> type) {
        Object result = currentRow.get(columnIndex - 1);
        setWasNull(null == result);
        return result;
    }
    
    @Override
    public Object getCalendarValue(final int columnIndex, final Class<?> type, final Calendar calendar) {
        Object result = currentRow.get(columnIndex - 1);
        setWasNull(null == result);
        return result;
    }
}
