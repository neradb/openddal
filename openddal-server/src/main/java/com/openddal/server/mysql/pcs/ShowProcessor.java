package com.openddal.server.mysql.pcs;

import static com.alibaba.druid.sql.visitor.SQLEvalVisitor.EVAL_VALUE;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlEvalVisitorImpl;
import com.alibaba.druid.sql.visitor.SQLEvalVisitor;
import com.alibaba.druid.sql.visitor.SQLEvalVisitorUtils;
import com.alibaba.druid.util.JdbcConstants;
import com.openddal.result.LocalResult;
import com.openddal.result.SimpleResultSet;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;

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
                MySqlShowDatabasesStatement s = (MySqlShowDatabasesStatement) stmts.get(0);
            } else if (stmt instanceof MySqlShowVariantsStatement) {
                MySqlShowVariantsStatement s = (MySqlShowVariantsStatement) stmt;
                return showVariables(s);
            } else if (stmt instanceof SQLShowTablesStatement) {
                SQLShowTablesStatement s = (SQLShowTablesStatement) stmt;
            } else if (stmt instanceof MySqlShowTableStatusStatement) {
                MySqlShowTableStatusStatement s = (MySqlShowTableStatusStatement) stmt;
            } else if (stmt instanceof MySqlShowTableStatusStatement) {
                MySqlShowTableStatusStatement s = (MySqlShowTableStatusStatement) stmt;
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
                    SQLIdentifierExpr col = new SQLIdentifierExpr("VARIABLE_NAME");
                    SQLBinaryOpExpr filter = new SQLBinaryOpExpr(col, SQLBinaryOperator.Like, like);
                }

            }
        }
        //we have to convert result.
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

}
