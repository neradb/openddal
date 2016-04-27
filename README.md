# OpenDDAL
OpenDDAL基于[jdbc-shards](https://github.com/wplatform/jdbc-shards),致力于提供一个简单易用，高性能的分布式数据库访问组件。

# RoadMap
#### jdbc-shards 1.x
- 提供基于JDBC API访问，实现分库分表
- 表路由规则，路由计算
- 分库查询结果合并，排序，10000以内的limt支持
- 支持跨结点查询，支持join,count,order by,group by以及聚合函数等的SQL持性
- 读写分离支持，HA支持，问题数据库隔离和自动恢复

#### OpenDDAL1.x
- 简化sql parser，去除无用语句语法支持，ddl语句仅支持创建，修改表和索引
- 精简sql engine xml配置,提供编程式创建sql engine实例
- 实现基于成本的分布式SQL优化器，以优化的方式执行sql
- 实现分布式执行行计划，便用分析sql的执行效率
- 引入puniverse－comsat，异步化后端jdbc调用，提高吞吐量
- SQL统计信息，实时查询运行SQL的统计数据
