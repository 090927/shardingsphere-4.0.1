> 基于 JDK SPI 机制, 实现扩展性

- SPI 核心类 `TypeBasedSPI`


- 分布式主键生成方案 `ShardingKeyGenerator`
  1. 雪花算法那
  2. UUID
  
> 分片引擎

* [x] SQL 解析引擎 （使用外观模式）
  - `SQLParseEngine` 核心类，同时委托 `SQLParseKernel`
* [x] SQL 路由
  -`ShardingRouter` 核心类, 
  - `RoutingEngineFactory` 负责生成这些具体的 `RoutingEngine`（分片路由）
* [x] SQL 改写
  - `SQLRewriteEngine`核心类
* [x] SQL 执行
  - `ShardingExecuteEngine` 分片执行引入的入口类
* [x] 归并引擎
  - `MergeEngine`
    1. 核心 `DQLMergeEngine#merge()`

> ShardingSphere 链路追踪

1. 使用 `OpenTracing API` 发送性能追踪数据。 
2. 初始化类 `ShardingTracer#init()`