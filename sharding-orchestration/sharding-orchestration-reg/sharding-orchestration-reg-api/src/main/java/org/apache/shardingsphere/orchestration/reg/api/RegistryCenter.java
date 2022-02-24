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

package org.apache.shardingsphere.orchestration.reg.api;

import org.apache.shardingsphere.orchestration.reg.listener.DataChangedEventListener;
import org.apache.shardingsphere.spi.TypeBasedSPI;

import java.util.List;

/**
 * Registry center.
 * 
 * @author zhangliang
 * @author zhaojun
 */
public interface RegistryCenter extends TypeBasedSPI {
    
    /**
     * Initialize registry center.
     * 
     * @param config registry center configuration
     *
     *               根据配置信息初始化注册中心
     */
    void init(RegistryCenterConfiguration config);
    
    /**
     * Get data from registry center.
     * 
     * <p>Maybe use cache if existed.</p>
     * 
     * @param key key of data
     * @return value of data
     *
     *  获取数据
     */
    String get(String key);
    
    /**
     * Get data from registry center directly.
     * 
     * <p>Cannot use cache.</p>
     *
     * @param key key of data
     * @return value of data
     *
     *   直接获取数据
     */
    String getDirectly(String key);
    
    /**
     * Judge data is existed or not.
     * 
     * @param key key of data
     * @return data is existed or not
     *
     *  是否存在数据项
     */
    boolean isExisted(String key);
    
    /**
     * Get node's sub-nodes list.
     *
     * @param key key of data
     * @return sub-nodes name list
     *
     *  获取子数据项列表
     */
    List<String> getChildrenKeys(String key);
    
    /**
     * Persist data.
     * 
     * @param key key of data
     * @param value value of data
     *
     *      持久化数据项
     */
    void persist(String key, String value);
    
    /**
     * Update data.
     *
     * @param key key of data
     * @param value value of data
     *
     *       更新数据项
     */
    void update(String key, String value);
    
    /**
     * Persist ephemeral data.
     *
     * @param key key of data
     * @param value value of data
     *
     *    持久化临时数据
     */
    void persistEphemeral(String key, String value);
    
    /**
     * Watch key or path of the registry.
     *
     * @param key key of data
     * @param dataChangedEventListener data changed event listener
     *
     *      对数据项或路径进行监听
     */
    void watch(String key, DataChangedEventListener dataChangedEventListener);
    
    /**
     * Close.
     *
     *  关闭注册中心
      */
    void close();

    /**
     * Initialize the lock of the key.
     *
     * @param key key of data
     *
     *            对数据项，初始化锁
     */
    void initLock(String key);

    /**
     * Try to get the lock of the key.
     *
     * @return get the lock or not
     *
     *  对数据项获取锁
     */
    boolean tryLock();

    /**
     * Try to release the lock of the key.
     *
     *  对数据项，释放锁
     *
     */
    void tryRelease();
}
