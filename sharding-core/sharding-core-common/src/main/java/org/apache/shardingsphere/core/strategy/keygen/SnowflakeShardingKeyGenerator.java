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

package org.apache.shardingsphere.core.strategy.keygen;

import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import org.apache.shardingsphere.spi.keygen.ShardingKeyGenerator;

import java.util.Calendar;
import java.util.Properties;

/**
 * Snowflake distributed primary key generator.
 * 
 * <p>
 * Use snowflake algorithm. Length is 64 bit.
 * </p>
 * 
 * <pre>
 * 1bit sign bit.
 * 41bits timestamp offset from 2016.11.01(ShardingSphere distributed primary key published data) to now.
 * 10bits worker process id.
 * 12bits auto increment offset in one mills
 * </pre>
 * 
 * <p>
 * Call @{@code SnowflakeShardingKeyGenerator.setWorkerId} to set worker id, default value is 0.
 * </p>
 * 
 * <p>
 * Call @{@code SnowflakeShardingKeyGenerator.setMaxTolerateTimeDifferenceMilliseconds} to set max tolerate time difference milliseconds, default value is 0.
 * </p>
 * 
 * @author gaohongtao
 * @author panjuan
 *
 *  雪花算法
 */
public final class SnowflakeShardingKeyGenerator implements ShardingKeyGenerator {
    
    public static final long EPOCH;
    
    private static final long SEQUENCE_BITS = 12L;
    
    private static final long WORKER_ID_BITS = 10L;
    
    private static final long SEQUENCE_MASK = (1 << SEQUENCE_BITS) - 1;
    
    private static final long WORKER_ID_LEFT_SHIFT_BITS = SEQUENCE_BITS;
    
    private static final long TIMESTAMP_LEFT_SHIFT_BITS = WORKER_ID_LEFT_SHIFT_BITS + WORKER_ID_BITS;
    
    private static final long WORKER_ID_MAX_VALUE = 1L << WORKER_ID_BITS;
    
    private static final long WORKER_ID = 0;
    
    private static final int DEFAULT_VIBRATION_VALUE = 1;
    
    private static final int MAX_TOLERATE_TIME_DIFFERENCE_MILLISECONDS = 10;
    
    @Setter
    private static TimeService timeService = new TimeService();
    
    @Getter
    @Setter
    private Properties properties = new Properties();
    
    private int sequenceOffset = -1;
    
    private long sequence;
    
    private long lastMilliseconds;
    
    static {
        Calendar calendar = Calendar.getInstance();
        calendar.set(2016, Calendar.NOVEMBER, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        EPOCH = calendar.getTimeInMillis();
    }
    
    @Override
    public String getType() {
        return "SNOWFLAKE";
    }
    
    @Override
    public synchronized Comparable<?> generateKey() {

        //获取当前时间戳
        long currentMilliseconds = timeService.getCurrentMillis();

        // 如果出现了时钟回拨，则抛出异常或进行时钟等待
        if (waitTolerateTimeDifferenceIfNeed(currentMilliseconds)) {
            currentMilliseconds = timeService.getCurrentMillis();
        }

        // 如果上次的生成时间与本次的是同一毫秒
        if (lastMilliseconds == currentMilliseconds) {

            // 这个位运算保证始终就是在4096这个范围内，避免你自己传递的sequence超过了4096这个范围
            if (0L == (sequence = (sequence + 1) & SEQUENCE_MASK)) {

                // 如果位运算结果为0，则需要等待下一个毫秒继续生成
                currentMilliseconds = waitUntilNextTime(currentMilliseconds);
            }
        } else {

            // 如果不是，则生成新的 sequence
            vibrateSequenceOffset();
            sequence = sequenceOffset;
        }
        lastMilliseconds = currentMilliseconds;

        // 先将当前时间戳左移放到完成 41个bit，然后将工作进程为左移到 10个bit，再将序号为放到最后的 12个bit
        // 最后拼接起来成一个64 bit的二进制数字
        return ((currentMilliseconds - EPOCH) << TIMESTAMP_LEFT_SHIFT_BITS) | (getWorkerId() << WORKER_ID_LEFT_SHIFT_BITS) | sequence;
    }
    
    @SneakyThrows
    private boolean waitTolerateTimeDifferenceIfNeed(final long currentMilliseconds) {
        if (lastMilliseconds <= currentMilliseconds) {
            return false;
        }
        long timeDifferenceMilliseconds = lastMilliseconds - currentMilliseconds;
        Preconditions.checkState(timeDifferenceMilliseconds < getMaxTolerateTimeDifferenceMilliseconds(), 
                "Clock is moving backwards, last time is %d milliseconds, current time is %d milliseconds", lastMilliseconds, currentMilliseconds);
        Thread.sleep(timeDifferenceMilliseconds);
        return true;
    }
    
    private long getWorkerId() {
        long result = Long.valueOf(properties.getProperty("worker.id", String.valueOf(WORKER_ID)));
        Preconditions.checkArgument(result >= 0L && result < WORKER_ID_MAX_VALUE);
        return result;
    }
    
    private int getMaxVibrationOffset() {
        int result = Integer.parseInt(properties.getProperty("max.vibration.offset", String.valueOf(DEFAULT_VIBRATION_VALUE)));
        Preconditions.checkArgument(result >= 0 && result <= SEQUENCE_MASK, "Illegal max vibration offset");
        return result;
    }
    
    private int getMaxTolerateTimeDifferenceMilliseconds() {
        return Integer.valueOf(properties.getProperty("max.tolerate.time.difference.milliseconds", String.valueOf(MAX_TOLERATE_TIME_DIFFERENCE_MILLISECONDS)));
    }
    
    private long waitUntilNextTime(final long lastTime) {
        long result = timeService.getCurrentMillis();
        while (result <= lastTime) {
            result = timeService.getCurrentMillis();
        }
        return result;
    }
    
    private void vibrateSequenceOffset() {
        sequenceOffset = sequenceOffset >= getMaxVibrationOffset() ? 0 : sequenceOffset + 1;
    }
}
