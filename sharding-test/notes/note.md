> 基于 JDK SPI 机制, 实现扩展性

- SPI 核心类 `TypeBasedSPI`


- 分布式主键生成方案 `ShardingKeyGenerator`
  1. 雪花算法那
  2. UUID
  
> 分片引擎

- SQL 解析引擎 （使用外观模式）
  - `SQLParseEngine` 核心类，同时委托 `SQLParseKernel`