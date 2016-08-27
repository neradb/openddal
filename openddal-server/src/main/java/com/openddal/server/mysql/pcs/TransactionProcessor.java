package com.openddal.server.mysql.pcs;

import java.util.List;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLReleaseSavePointStatement;
import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.druid.sql.ast.statement.SQLSavePointStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCommitStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlStartTransactionStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.openddal.engine.Session;
import com.openddal.server.ServerException;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;
import com.openddal.server.util.ErrorCode;
import com.openddal.server.util.StringUtil;
/**
 * @author jorgie.li
 */
public final class TransactionProcessor implements QueryProcessor {

    private DefaultQueryProcessor target;

    public TransactionProcessor(DefaultQueryProcessor target) {
        this.target = target;
    }

    @Override
    public QueryResult process(String query) {
        QueryResult result = new QueryResult(0);
        List<SQLStatement> stmts = SQLUtils.parseStatements(query, JdbcConstants.MYSQL);
        Session session = target.getSession().getDbSession();
        for (SQLStatement stmt : stmts) {
            if (stmt instanceof MySqlStartTransactionStatement) {
                session.begin();
            } else if (stmt instanceof MySqlCommitStatement) {
                session.commit();
            } else if (stmt instanceof SQLRollbackStatement) {
                SQLRollbackStatement s = (SQLRollbackStatement) stmt;
                if (s.getTo() != null) {
                    String savepoint = SQLUtils.toMySqlString(s.getTo());
                    session.rollbackToSavepoint(savepoint);
                } else {
                    session.rollback();
                }
            } else if (stmt instanceof SQLSavePointStatement) {
                SQLSavePointStatement s = (SQLSavePointStatement) stmt;
                String savepoint = SQLUtils.toMySqlString(s.getName());
                session.addSavepoint(savepoint);
            } else if (stmt instanceof SQLReleaseSavePointStatement) {
                SQLReleaseSavePointStatement s = (SQLReleaseSavePointStatement) stmt;
                String savepoint = SQLUtils.toMySqlString(s.getName());
                if (StringUtil.isEmpty(savepoint)) {
                    throw ServerException.get(ErrorCode.ER_BAD_NULL_ERROR, "savepoint name is null.");
                }
                session.releaseSavepoint(savepoint);
            } else {
                throw ServerException.get(ErrorCode.ER_NOT_ALLOWED_COMMAND, "not allowed command:" + query);
            }
        }

        return result;
    }

}