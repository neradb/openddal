package com.openddal.repo.mysql;

import com.openddal.engine.Database;
import com.openddal.executor.works.WorkerFactory;
import com.openddal.repo.JdbcRepository;
import com.openddal.repo.JdbcWorkerFactory;
import com.openddal.repo.SQLTranslator;

public class MySQLRepository extends JdbcRepository {

    private MySQLTranslator sqlTranslator;
    private JdbcWorkerFactory handlerFactory;

    @Override
    public WorkerFactory getWorkerFactory() {
        return handlerFactory;
    }

    @Override
    public SQLTranslator getSQLTranslator() {
        return sqlTranslator;
    }

    @Override
    public void init(Database database) {
        super.init(database);
        sqlTranslator = new MySQLTranslator(database);
        handlerFactory = new JdbcWorkerFactory(this);
    }

    @Override
    public String getName() {
        return "MYSQL_JDBC_REPOSITORY";
    }



}
