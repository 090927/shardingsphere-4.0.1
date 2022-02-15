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

package org.apache.shardingsphere.spi.encrypt;

import org.apache.shardingsphere.spi.TypeBasedSPI;

/**
 * Sharding encryptor.
 *
 * @author panjuan
 *
 *  实现类
 *   aes {@link org.apache.shardingsphere.core.strategy.encrypt.impl.AESShardingEncryptor}
 *   mdb {@link org.apache.shardingsphere.core.strategy.encrypt.impl.MD5ShardingEncryptor}
 */
public interface ShardingEncryptor extends TypeBasedSPI {
    
    /**
     * Initialize.
     * 初始化
     */
    void init();
    
    /**
     * Encode.
     * 加密
     * 
     * @param plaintext plaintext
     * @return ciphertext
     */
    String encrypt(Object plaintext);
    
    /**
     * Decode.
     * 解密
     * 
     * @param ciphertext ciphertext
     * @return plaintext
     */
    Object decrypt(String ciphertext);
}
