# OpenDDAL
OpenDDAL基于jdbc-shards[https://github.com/wplatform/jdbc-shards],致力于提供一个简单易用，高性能的分布式数据库访问组件。

# RoadMap
- 使用druid的sql parser,更好的兼容主流的数据库MySql,Oracle,DB2,PostgreSQL
- 优化分布式SQL引擎，简化设计，去掉从h2database引入代码，
- 加入server模式支持，基于netty实现mysql协议的server模式
- 引入puniverse－comsat，异步化后端jdbc调用，提吞吐量
