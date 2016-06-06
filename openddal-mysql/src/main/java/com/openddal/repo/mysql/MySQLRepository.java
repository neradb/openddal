package com.openddal.repo.mysql;

import com.openddal.engine.Database;
import com.openddal.excutor.handle.QueryHandlerFactory;
import com.openddal.repo.JdbcQueryHandlerFactory;
import com.openddal.repo.JdbcRepository;
import com.openddal.repo.SQLTranslator;

public class MySQLRepository extends JdbcRepository {

    private MySQLTranslator sqlTranslator;
    private JdbcQueryHandlerFactory handlerFactory;

    @Override
    public QueryHandlerFactory getQueryHandlerFactory() {
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
        handlerFactory = new JdbcQueryHandlerFactory(this);
    }

    @Override
    public String getName() {
        return "MYSQL_JDBC_REPOSITORY";
    }



}
