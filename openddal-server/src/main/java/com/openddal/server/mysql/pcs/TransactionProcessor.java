package com.openddal.server.mysql.pcs;

import java.util.List;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCommitStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRollbackStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;

public final class TransactionProcessor implements QueryProcessor {

    private DefaultQueryProcessor target;

    public TransactionProcessor(DefaultQueryProcessor target) {
        this.target = target;
    }

    @Override
    public QueryResult process(String query) {
        List<SQLStatement> stmts = SQLUtils.parseStatements(query, JdbcConstants.MYSQL);
        for (SQLStatement stmt : stmts) {
            if (stmt instanceof MySqlRollbackStatement) {
                MySqlRollbackStatement s = (MySqlRollbackStatement) stmts.get(0);
            } else if (stmt instanceof MySqlCommitStatement) {
                MySqlCommitStatement s = (MySqlCommitStatement) stmt;
            }
        }

        return null;
    }

}