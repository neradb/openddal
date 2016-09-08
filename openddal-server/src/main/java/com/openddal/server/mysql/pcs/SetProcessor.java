package com.openddal.server.mysql.pcs;

import java.sql.Connection;
import java.util.List;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
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
/**
 * @author jorgie.li
 */
public final class SetProcessor implements QueryProcessor {

    private DefaultQueryProcessor target;

    public SetProcessor(DefaultQueryProcessor target) {
        this.target = target;
    }

    @Override
    public QueryResult process(String query) {
        QueryResult result = new QueryResult(0);
        List<SQLStatement> stmts = SQLUtils.parseStatements(query, JdbcConstants.MYSQL);
        for (SQLStatement stmt : stmts) {
            if (stmt instanceof MySqlSetTransactionStatement) {
                MySqlSetTransactionStatement s = (MySqlSetTransactionStatement) stmt;
                String isolationLevel = s.getIsolationLevel();
                String accessModel = s.getAccessModel();
                if (!StringUtil.isEmpty(isolationLevel)) {
                    setIsolation(isolationLevel);
                }
                if (!StringUtil.isEmpty(accessModel)) {
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
                result.setWarnings((short) 1);
                result.setMessage("Set password statement ignored.");
                continue;
            } else {
                SQLSetStatement s = (SQLSetStatement) stmt;
                List<SQLAssignItem> items = s.getItems();
                boolean isGlobal = false;
                for (int i = 0; i < items.size(); i++) {
                    SQLAssignItem item = items.get(0);
                    SQLVariantRefExpr varRef = (SQLVariantRefExpr) item.getTarget();
                    isGlobal = i == 0 ? varRef.isGlobal() : isGlobal;
                    String key = varRef.getName();
                    String value = SQLUtils.toMySqlString(item.getValue());
                    if ("autocommit".equalsIgnoreCase(key)) {
                        setAutocommit(value);
                    } else if ("tx_isolation".equalsIgnoreCase(key)) {
                        setIsolation(value);
                    } else if ("tx_read_only".equalsIgnoreCase(key)) {
                        setReadOnly(value);
                    } else {
                        result.setWarnings((short) 1);
                        result.setMessage(query + " ignored.");
                    }
                    
                }
            }
        }
        return result;

    }


    private void setIsolation(String isolation) {
        Session session = target.getSession().getDbSession();
        if (isolation.matches("REPEATABLE[\\s|\\-]READ")) {
            session.setIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        } else if (isolation.matches("READ[\\s|\\-]COMMITTED")) {
            session.setIsolation(Connection.TRANSACTION_READ_COMMITTED);
        } else if (isolation.matches("READ[\\s|\\-]UNCOMMITTED")) {
            session.setIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        } else if ("SERIALIZABLE".equalsIgnoreCase(isolation)) {
            session.setIsolation(Connection.TRANSACTION_SERIALIZABLE);
        } else {
            throw ServerException.get(ErrorCode.ER_SYNTAX_ERROR, isolation);
        }

    }

    private void setAccessModel(String accessModel) {
        Session session = target.getSession().getDbSession();
        if ("WRITE".equals(accessModel)) {
            session.setReadOnly(false);
        } else if ("ONLY".equals(accessModel)) {
            session.setReadOnly(true);
        } else {
            throw ServerException.get(ErrorCode.ER_SYNTAX_ERROR, accessModel);
        }
    }

    private void setReadOnly(String readOnly) {
        Session session = target.getSession().getDbSession();
        if (readOnly.matches("[0|OFF|FALSE]")) {
            session.setReadOnly(false);
        } else if (readOnly.matches("[1|NO|TRUE]")) {
            session.setReadOnly(true);
        }

    }

    private void setAutocommit(String autocommit) {
        Session session = target.getSession().getDbSession();
        if (autocommit.matches("[0|OFF|FALSE]")) {
            session.setAutoCommit(false);
        } else if (autocommit.matches("[1|NO|TRUE]")) {
            session.setAutoCommit(true);
        }
    }

    private void setCharset(String charset) {
        ServerSession session = target.getSession();
        session.setCharset(charset);
    }

}
