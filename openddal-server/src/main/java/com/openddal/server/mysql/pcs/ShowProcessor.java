package com.openddal.server.mysql.pcs;

import static com.alibaba.druid.sql.visitor.SQLEvalVisitor.EVAL_VALUE;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowAuthorsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlEvalVisitorImpl;
import com.alibaba.druid.sql.visitor.SQLEvalVisitor;
import com.alibaba.druid.sql.visitor.SQLEvalVisitorUtils;
import com.alibaba.druid.util.JdbcConstants;
import com.openddal.engine.Constants;
import com.openddal.result.LocalResult;
import com.openddal.result.SimpleResultSet;
import com.openddal.server.ServerException;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;
import com.openddal.server.util.ErrorCode;

/**
 * @author jorgie.li
 */
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
                return showDatabase((MySqlShowDatabasesStatement) stmt);
            } else if (stmt instanceof MySqlShowVariantsStatement) {
                return showVariables((MySqlShowVariantsStatement) stmt);
            } else if (stmt instanceof SQLShowTablesStatement) {
                return showTables((SQLShowTablesStatement) stmt);
            } else if (stmt instanceof MySqlShowAuthorsStatement) {
                return showAuthors((MySqlShowAuthorsStatement) stmt);
            } else if (stmt instanceof MySqlShowCharacterSetStatement) {
                return showCharacterSet((MySqlShowCharacterSetStatement) stmt);
            } else if (stmt instanceof MySqlShowCollationStatement) {
                return showCollation((MySqlShowCollationStatement) stmt);
            } else if (stmt instanceof MySqlShowColumnsStatement) {
                return showColumns((MySqlShowColumnsStatement) stmt);
            } else if (stmt instanceof MySqlShowTableStatusStatement) {
                throw ServerException.get(ErrorCode.ER_NOT_ALLOWED_COMMAND, "not allowed command:" + query);
            }
        }

        return null;
    }

    private QueryResult showVariables(MySqlShowVariantsStatement s) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Variable_name", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Value", Types.VARCHAR, Integer.MAX_VALUE, 0);
        Map<String, String> variables = target.getSession().getServer().getVariables();
        SQLCharExpr like = (SQLCharExpr) s.getLike();
        if (like != null) {
            for (Map.Entry<String, String> item : variables.entrySet()) {
                String key = item.getKey();
                String value = item.getValue();
                if (SQLEvalVisitorUtils.like(key, like.getText())) {
                    result.addRow(new Object[] { key, value });
                }

            }
        }
        SQLExpr where = s.getWhere();
        if (where != null) {
            for (Map.Entry<String, String> item : variables.entrySet()) {
                final String key = item.getKey();
                final String value = item.getValue();
                SQLEvalVisitor visitor = new MySqlEvalVisitorImpl() {
                    public boolean visit(SQLIdentifierExpr x) {
                        String name = x.getName();
                        if ("Variable_name".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, key);
                        } else if ("Value".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, value);
                        }
                        return false;
                    }
                };
                where.accept(visitor);
                Object b = where.getAttributes().get(EVAL_VALUE);
                boolean added = b instanceof Boolean ? (Boolean) b : false;
                if (added) {
                    result.addRow(new Object[] { key, value });
                }

            }
        }
        // we have to convert result.
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showDatabase(MySqlShowDatabasesStatement s) {
        StringBuilder sql = new StringBuilder("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ");
        SQLExpr like = s.getLike();
        SQLExpr where = s.getWhere();
        if (like != null) {
            sql.append(" WHERE SCHEMA_NAME").append("LIKE ").append(SQLUtils.toMySqlString(like));
        } else if (where != null) {
            sql.append(" WHERE ").append(SQLUtils.toMySqlString(where));
        }
        return target.process(sql.toString());
    }

    private QueryResult showTables(SQLShowTablesStatement s) {
        StringBuilder sql = new StringBuilder(
                "SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ");
        String schema = Constants.SCHEMA_MAIN;
        SQLName database = s.getDatabase();
        SQLExpr like = s.getLike();
        SQLExpr where = s.getWhere();
        if (database != null) {
            schema = SQLUtils.toMySqlString(database);
        }
        sql.append("'").append(schema).append("'");

        if (like != null) {
            sql.append(" AND TABLE_NAME").append("LIKE ").append(SQLUtils.toMySqlString(database));
        } else if (where != null) {
            sql.append(" AND ").append(SQLUtils.toMySqlString(where));
        }
        sql.append(" ORDER BY TABLE_NAME");
        return target.process(sql.toString());
    }

    private QueryResult showColumns(MySqlShowColumnsStatement s) {
        // not support like and where
        s.setLike(null);
        s.setWhere(null);
        return target.process(SQLUtils.toMySqlString(s));
    }

    private QueryResult showAuthors(MySqlShowAuthorsStatement s) {

        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Variable_name", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Value", Types.VARCHAR, Integer.MAX_VALUE, 0);

        // we have to convert result.
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));

    }

    private QueryResult showCharacterSet(MySqlShowCharacterSetStatement s) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Charset", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Description", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Default collation", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Maxlen", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Description", Types.INTEGER, Integer.MAX_VALUE, 0);
        SQLExpr like = s.getPattern();
        SQLExpr where = s.getWhere();
        if (like != null) {

        } else if (where != null) {

        }
        // we have to convert result.
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showCollation(MySqlShowCollationStatement s) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Charset", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Collation", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Id", Types.INTEGER, Integer.MAX_VALUE, 0);
        result.addColumn("Default", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Compiled", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Sortlen", Types.INTEGER, Integer.MAX_VALUE, 0);
        SQLExpr like = s.getPattern();
        SQLExpr where = s.getWhere();
        if (like != null) {

        } else if (where != null) {

        }
        // we have to convert result.
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

}
