package com.openddal.server.mysql.pcs;

import static com.alibaba.druid.sql.visitor.SQLEvalVisitor.EVAL_VALUE;

import java.sql.Connection;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlExprParser;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlLexer;
import com.alibaba.druid.sql.parser.Token;
import com.openddal.engine.Session;
import com.openddal.result.LocalResult;
import com.openddal.result.SimpleResultSet;
import com.openddal.server.NettyServer;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;
import com.openddal.server.mysql.MySQLServer;
import com.openddal.server.util.StringUtil;
import com.openddal.util.New;
/**
 * @author jorgie.li
 */
public final class SelectProcessor implements QueryProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectProcessor.class);

    private DefaultQueryProcessor target;

    public SelectProcessor(DefaultQueryProcessor target) {
        this.target = target;
    }

    @Override
    public QueryResult process(String query) {
        QueryResult result = localProcess(query);
        if (result == null) {
            result = target.process(query);
        }
        return result;
    }

    public QueryResult localProcess(String query) {
        List<SQLSelectItem> selectList = parseLocalItem(query);
        if (selectList == null) {
            return null;
        }
        SimpleResultSet result = new SimpleResultSet();
        Object[] values = new Object[selectList.size()];
        int index = 0;
        for (SQLSelectItem item : selectList) {
            SQLExpr expr = item.getExpr();
            String alias = item.getAlias();
            String name = SQLUtils.toMySqlString(expr);
            Object attribute = expr.getAttribute(EVAL_VALUE);
            result.addColumn(StringUtil.isEmpty(alias) ? name : alias,
                    attribute instanceof Number ? Types.NUMERIC : Types.VARCHAR, 0, 0);
            values[index++] = attribute;
        }
        result.addRow(values);
        // we have to convert result.
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private List<SQLSelectItem> parseLocalItem(String query) {
        List<SQLSelectItem> selectList = New.arrayList();
        try {
            MySqlLexer lexer = new MySqlLexer(query);
            MySqlExprParser parser = new MySqlExprParser(lexer);
            lexer.nextToken();
            if (lexer.token() == Token.SELECT) {
                lexer.nextToken();
                if (lexer.token() == Token.COMMENT) {
                    lexer.nextToken();
                }
                if (lexer.token() == Token.DISTINCT) {
                    lexer.nextToken();
                } else if (lexer.token() == Token.UNIQUE) {
                    lexer.nextToken();
                } else if (lexer.token() == Token.ALL) {
                    lexer.nextToken();
                }
                for (;;) {
                    final SQLSelectItem selectItem = parser.parseSelectItem();
                    SQLExpr expr = selectItem.getExpr();
                    if (expr instanceof SQLMethodInvokeExpr && initValue((SQLMethodInvokeExpr) expr)) {
                        selectList.add(selectItem);
                    } else if (expr instanceof SQLVariantRefExpr && initValue((SQLVariantRefExpr) expr)) {
                        selectList.add(selectItem);
                    } else if (expr instanceof SQLPropertyExpr && initValue((SQLPropertyExpr) expr)) {
                        selectList.add(selectItem);
                    } else {
                        return null;
                    }
                    if (lexer.token() != Token.COMMA) {
                        break;
                    }
                    lexer.nextToken();
                }
                return selectList;
            }
        } catch (Throwable e) {
            LOGGER.warn("parse select type error.", e);
        }
        return null;
    }

    private boolean initValue(SQLPropertyExpr expr) {
        if(expr.getOwner() instanceof SQLVariantRefExpr) {
            String name = expr.getName().toLowerCase();
            setValue(expr, name);
            return true;
        }
        return false;
    }

    private boolean initValue(SQLMethodInvokeExpr expr) {
        Session session = target.getSession().getDbSession();
        List<SQLExpr> parameters = expr.getParameters();
        if (!parameters.isEmpty()) {
            return false;
        }
        String name = expr.getMethodName().toLowerCase();
        if ("user".equals(name) || "current_user".equals(name)) {
            expr.putAttribute(EVAL_VALUE, session.getUser().getName());
            return true;
        } else if ("connection_id".equals(name)) {
            long threadId = target.getSession().getThreadId();
            expr.putAttribute(EVAL_VALUE, threadId);
            return true;
        } else if ("version_comment".equals(name)) {
            expr.putAttribute(EVAL_VALUE, MySQLServer.VERSION_COMMENT);
            return true;
        }
        return false;
    }

    private boolean initValue(SQLVariantRefExpr expr) {
        String name = expr.getName().toLowerCase();
        name = name.replaceAll("@@", "");
        setValue(expr, name);
        return true;
    }

    /**
     * @param expr
     * @param name
     */
    private void setValue(SQLExpr expr, String name) {
        Session session = target.getSession().getDbSession();
        NettyServer server = target.getSession().getServer();
        Map<String, String> variables = server.getVariables();
        if ("autocommit".equals(name)) {
            int value = session.getAutoCommit() ? 1 : 0;
            expr.putAttribute(EVAL_VALUE, String.valueOf(value));
        } else if ("tx_isolation".equals(name)) {
            int isolation = session.getIsolation();
            switch (isolation) {
            case Connection.TRANSACTION_NONE:
                expr.putAttribute(EVAL_VALUE, variables.get(name));
                break;
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                expr.putAttribute(EVAL_VALUE, "READ-UNCOMMITTED");
                break;
            case Connection.TRANSACTION_READ_COMMITTED:
                expr.putAttribute(EVAL_VALUE, "READ-COMMITTED");
                break;
            case Connection.TRANSACTION_REPEATABLE_READ:
                expr.putAttribute(EVAL_VALUE, "REPEATABLE-READ");
                break;
            case Connection.TRANSACTION_SERIALIZABLE:
                expr.putAttribute(EVAL_VALUE, "SERIALIZABLE");
                break;
            default:
            }
        } else if ("tx_read_only".equals(name)) {
            int value = session.isReadOnly() ? 1 : 0;
            expr.putAttribute(EVAL_VALUE, String.valueOf(value));
        } else if ("version_comment".equals(name)) {
            expr.putAttribute(EVAL_VALUE, MySQLServer.VERSION_COMMENT);
        } else {
            expr.putAttribute(EVAL_VALUE, variables.get(name));
        }
    }

}
