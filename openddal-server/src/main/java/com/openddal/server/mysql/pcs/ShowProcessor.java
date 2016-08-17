package com.openddal.server.mysql.pcs;

import java.util.List;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;

public final class ShowProcessor implements QueryProcessor {

    private DefaultQueryProcessor target;

    public ShowProcessor(DefaultQueryProcessor target) {
        this.target = target;
    }

    @Override
    public QueryResult process(String query) {
        List<SQLStatement> stmts = SQLUtils.parseStatements(query, JdbcConstants.MYSQL);
        for (SQLStatement stmt : stmts) {
            if (stmt instanceof MySqlShowDatabasesStatement) {
                MySqlShowDatabasesStatement s = (MySqlShowDatabasesStatement) stmts.get(0);
            } else if (stmt instanceof MySqlShowTableStatusStatement) {
                MySqlShowTableStatusStatement s = (MySqlShowTableStatusStatement) stmt;
            } else if (stmt instanceof MySqlShowTableStatusStatement) {
                MySqlShowTableStatusStatement s = (MySqlShowTableStatusStatement) stmt;
            } else if (stmt instanceof MySqlShowTableStatusStatement) {
                MySqlShowTableStatusStatement s = (MySqlShowTableStatusStatement) stmt;
            } else if (stmt instanceof MySqlShowTableStatusStatement) {
                MySqlShowTableStatusStatement s = (MySqlShowTableStatusStatement) stmt;
            }
        }

        return null;
    }

}
