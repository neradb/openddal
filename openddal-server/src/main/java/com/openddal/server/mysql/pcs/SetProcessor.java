package com.openddal.server.mysql.pcs;

import java.sql.Connection;
import java.util.List;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetCharSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetNamesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetPasswordStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.openddal.engine.Session;
import com.openddal.server.ServerException;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;
import com.openddal.server.core.ServerSession;
import com.openddal.server.util.ErrorCode;
import com.openddal.server.util.StringUtil;

public final class SetProcessor implements QueryProcessor {

    private DefaultQueryProcessor target;

    public SetProcessor(DefaultQueryProcessor target) {
        this.target = target;
    }

    @Override
    public QueryResult process(String query) {
        List<SQLStatement> stmts = SQLUtils.parseStatements(query, JdbcConstants.MYSQL);
        for (SQLStatement stmt : stmts) {
            if (stmt instanceof MySqlSetTransactionStatement) {
                MySqlSetTransactionStatement s = (MySqlSetTransactionStatement) stmt;
                String isolationLevel = s.getIsolationLevel();
                String accessModel = s.getAccessModel();
                if (StringUtil.isEmpty(isolationLevel)) {
                    setIsolation(isolationLevel);
                }
                if (StringUtil.isEmpty(accessModel)) {
                    setAccessModel(accessModel);
                }
            } else if (stmt instanceof MySqlSetNamesStatement) {
                MySqlSetNamesStatement s = (MySqlSetNamesStatement) stmt;
                String charset = s.getCharSet();
                setCharset(charset);
            } else if (stmt instanceof MySqlSetCharSetStatement) {
                MySqlSetCharSetStatement s = (MySqlSetCharSetStatement) stmt;
                String charset = s.getCharSet();
                setCharset(charset);
            } else if (stmt instanceof MySqlSetPasswordStatement) {
                MySqlSetPasswordStatement s = (MySqlSetPasswordStatement) stmt;
            } else {
                SQLSetStatement s = (SQLSetStatement) stmt;
                List<SQLAssignItem> items = s.getItems();
                for (SQLAssignItem sqlAssignItem : items) {
                    String key = SQLUtils.toMySqlString(sqlAssignItem.getTarget());
                    String value = SQLUtils.toMySqlString(sqlAssignItem.getValue());
                }
            }
        }
        return new QueryResult(0);

    }

    private void setIsolation(String isolation) {
        Session session = target.getSession().getDatabaseSession();
        if ("REPEATABLE READ".equals(isolation)) {
            session.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        } else if ("READ COMMITTED".equals(isolation)) {
            session.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        } else if ("READ UNCOMMITTED".equals(isolation)) {
            session.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        } else if ("SERIALIZABLE".equals(isolation)) {
            session.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        }
        throw ServerException.get(ErrorCode.ER_SYNTAX_ERROR, isolation);

    }

    private void setAccessModel(String accessModel) {
        Session session = target.getSession().getDatabaseSession();
        if ("READ WRITE".equals(accessModel)) {
            session.setReadOnly(false);
        } else if ("READ ONLY".equals(accessModel)) {
            session.setReadOnly(false);
        }
        throw ServerException.get(ErrorCode.ER_SYNTAX_ERROR, accessModel);
    }

    private void setCharset(String charset) {
        ServerSession session = target.getSession();
        session.setCharset(charset);
    }

}
