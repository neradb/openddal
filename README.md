# OpenDDAL概述
OpenDDAL致力于提供一个简单易用，高性能的分布式数据库访问服务，OpenDDAL内致的分布式SQL引擎能处理各种复杂的SQL，为之选择可靠的执行计划。OpenDDAL提供SQL语句执行计划查询，方便的理解SQL的路由及执行。有助于开发可靠的业务代码。除此之外，OpenDDAL提供了简洁高效的切分实现，数据库隔离及恢复，读写分离等功能。

# OpenDDAL架构
![](https://raw.githubusercontent.com/wplatform/blog/master/assets/openddal_main/architecture.png)


# OpenDDAL文档中心
- [功能演示](https://github.com/wplatform/blog/blob/master/posts/openddal-func-showcase.md)
- [用户指南](https://github.com/wplatform/blog/blob/master/posts/openddal-guide.md)

# OpenDDAL功能优势
- 水平分库分表,提供简洁的切分方式，多纬切分，自定义切分规则
- 支持Embedded(JDBC)/Server(TCP/IP)两种运行模式
- 兼容MySQL协议，支持大部分MySQL客户端
- 支持结果集合排序，分组汇总，分页，还支持跨库join，子查询，union查询等复杂SQL
- 支持DDL，可以一次性对表结构，索引进行维护。
- 实现基于成本的分布式SQL优化器，为SQL提供可靠的执行方式
- 支持SQL语句执行计划查询，方便的理解SQL的路由及执行
- 实现了基于MySQL数据库的Repository层
- 读写分离支持，HA支持，问题数据库隔离和自动恢复
- 支持全局唯一序列，使用方式类似于Oracle，DB2中Sequence对象

# OpenDDAL需求规划
- 实现更多的Repository层，如PgSQL，Oracle，DB2
- 实现基于NoSQL的Repository层，如Mongodb,Hbase 

