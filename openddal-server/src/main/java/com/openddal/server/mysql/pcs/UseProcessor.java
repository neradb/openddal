package com.openddal.server.mysql.pcs;

import java.util.List;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.openddal.dbobject.schema.Schema;
import com.openddal.engine.Session;
import com.openddal.server.ServerException;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;
import com.openddal.server.util.ErrorCode;
/**
 * @author jorgie.li
 */
public final class UseProcessor implements QueryProcessor {

    private DefaultQueryProcessor target;

    public UseProcessor(DefaultQueryProcessor target) {
        this.target = target;
    }

    @Override
    public QueryResult process(String query) {
        Session session = target.getSession().getDbSession();
        QueryResult result = new QueryResult(0);
        List<SQLStatement> stmts = SQLUtils.parseStatements(query, JdbcConstants.MYSQL);
        for (SQLStatement stmt : stmts) {
            if (stmt instanceof SQLUseStatement) {
                SQLUseStatement s = (SQLUseStatement) stmt;
                String database = SQLUtils.toMySqlString(s.getDatabase());
                database = database.replaceAll("['|`]", "");
                Schema schema = session.getDatabase().findSchema(session.getDatabase().identifier(database));
                if (schema == null) {
                    throw ServerException.get(ErrorCode.ER_BAD_DB_ERROR, "Unknown database " + database);
                }
                result.setWarnings((short) 1);
                result.setMessage("The database is immutable, there is no need to use the 'use' command");
            } else {
                throw ServerException.get(ErrorCode.ER_NOT_ALLOWED_COMMAND, "not allowed command:" + query);

            }
        }
        return result;
    }

}