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

package org.apache.shardingsphere.sharding.merge.dql.orderby;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.shardingsphere.underlying.execute.QueryResult;
import org.apache.shardingsphere.underlying.merge.impl.StreamMergedResult;
import org.apache.shardingsphere.sql.parser.relation.segment.select.orderby.OrderByItem;

import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Stream merged result for order by.
 *
 * @author zhangliang
 *
 *  排序归并
 */
public class OrderByStreamMergedResult extends StreamMergedResult {
    
    private final Collection<OrderByItem> orderByItems;
    
    @Getter(AccessLevel.PROTECTED)
    private final Queue<OrderByValue> orderByValuesQueue;
    
    @Getter(AccessLevel.PROTECTED)
    private boolean isFirstNext;
    
    public OrderByStreamMergedResult(final List<QueryResult> queryResults, final Collection<OrderByItem> orderByItems) throws SQLException {
        this.orderByItems = orderByItems;

        // 构建 PriorityQueue
        this.orderByValuesQueue = new PriorityQueue<>(queryResults.size());

        /**
         * 初始化 PriorityQueue {@link #orderResultSetsToQueue(List)}
         */
        orderResultSetsToQueue(queryResults);
        isFirstNext = true;
    }
    
    private void orderResultSetsToQueue(final List<QueryResult> queryResults) throws SQLException {
        for (QueryResult each : queryResults) {

            // 构建 OrderByValue
            OrderByValue orderByValue = new OrderByValue(each, orderByItems);
            if (orderByValue.next()) {

                // 添加 OrderByValue 到队列中
                orderByValuesQueue.offer(orderByValue);
            }
        }
        setCurrentQueryResult(orderByValuesQueue.isEmpty() ? queryResults.get(0) : orderByValuesQueue.peek().getQueryResult());
    }

    /**
     *  当多个数据库中执行某一条 SQL 语句，我们做到在每个库内部完成排序,
     *  我们结果集中，保存着内部排好序的 QueryResult, 然后进行全局排序。
     *
     *   采用，优先级队列，每次获取下一条数据时，只需要将队列顶端结果集的游标下移。
     *   并根据 '新游标' 重新进入优先级队列，并找到自己的位置即可。
     */
    @Override
    public boolean next() throws SQLException {
        if (orderByValuesQueue.isEmpty()) {
            return false;
        }
        if (isFirstNext) {
            isFirstNext = false;
            return true;
        }

        //获取 PriorityQueue 中的第一个元素，并弹出该元素
        OrderByValue firstOrderByValue = orderByValuesQueue.poll();

        // 将游标指向 firstOrderByValue 的下一个元素，并重新插入到 PriorityQueue 中，这会促使 PriorityQueue 进行自动的重排序
        if (firstOrderByValue.next()) {
            orderByValuesQueue.offer(firstOrderByValue);
        }
        if (orderByValuesQueue.isEmpty()) {
            return false;
        }

        // 将当前结果集指向 PriorityQueue 的第一个元素
        setCurrentQueryResult(orderByValuesQueue.peek().getQueryResult());
        return true;
    }
}
