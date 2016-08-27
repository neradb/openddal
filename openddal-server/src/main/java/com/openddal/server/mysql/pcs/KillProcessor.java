package com.openddal.server.mysql.pcs;

import java.util.List;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlKillStatement.Type;
import com.alibaba.druid.util.JdbcConstants;
import com.openddal.engine.Session;
import com.openddal.server.NettyServer;
import com.openddal.server.ServerException;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;
import com.openddal.server.core.ServerSession;
import com.openddal.server.util.ErrorCode;
/**
 * @author jorgie.li
 */
public final class KillProcessor implements QueryProcessor {

    private DefaultQueryProcessor target;
    
    public KillProcessor(DefaultQueryProcessor target) {
        this.target = target;
    }

    @Override
    public QueryResult process(String query) {
        QueryResult result = new QueryResult(0);
        NettyServer server = target.getSession().getServer();
        List<SQLStatement> stmts = SQLUtils.parseStatements(query, JdbcConstants.MYSQL);
        for (SQLStatement stmt : stmts) {
            MySqlKillStatement s = (MySqlKillStatement) stmt;
            List<SQLExpr> threadIds = s.getThreadIds();
            for (SQLExpr sqlExpr : threadIds) {
                String sql = SQLUtils.toMySqlString(sqlExpr);
                long threadId = Long.parseLong(sql);
                ServerSession ss = server.getSession(threadId);
                if (ss != null && ss.getDbSession() != null) {
                    Session session = ss.getDbSession();
                    Type type = s.getType();
                    switch (type) {
                    case CONNECTION:
                        session.cancel();
                        ss.close();
                        break;
                    case QUERY:
                        session.cancel();
                        break;
                    default:
                        ServerException.get(ErrorCode.ER_ERROR_WHEN_EXECUTING_COMMAND, "error kill type.");
                    }
                }
            }
        }
        return result;
    }

}
