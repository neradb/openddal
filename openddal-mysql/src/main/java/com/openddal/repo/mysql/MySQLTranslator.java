package com.openddal.repo.mysql;

import java.util.ArrayList;
import java.util.List;

import com.openddal.command.Parser;
import com.openddal.command.dml.Select;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.engine.Database;
import com.openddal.repo.SQLTranslator;
import com.openddal.result.SortOrder;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;
import com.openddal.util.StatementBuilder;
import com.openddal.util.StringUtils;
import com.openddal.value.Value;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLTranslator implements SQLTranslator {

    private final Database database;

    public MySQLTranslator(Database database) {
        super();
        this.database = database;
    }

    @Override
    public String identifier(String identifier) {
        return database.identifier(identifier);
    }

    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/select.html
     */
    @Override
    public Result translate(Select select, ObjectNode executionOn) {
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        List<Value> params = New.arrayList(10);
        ArrayList<Expression> expressions = select.getExpressions();
        Expression[] exprList = expressions.toArray(new Expression[expressions.size()]);
        StatementBuilder buff = new StatementBuilder("SELECT");
        if (select.isDistinct()) {
            buff.append(" DISTINCT");
        }
        int visibleColumnCount = select.getColumnCount();
        for (int i = 0; i < visibleColumnCount; i++) {
            buff.appendExceptFirst(",");
            buff.append(' ');
            buff.append(exprList[i].getSQL());
        }
        buff.append(" FROM ");
        TableFilter filter = select.getTopTableFilter();
        if (filter != null) {
            buff.resetCount();
            int i = 0;
            do {
                buff.appendExceptFirst(" ");
                buff.append(getPlanSQL(filter, i++ > 0));
                filter = filter.getJoin();
            } while (filter != null);
        } else {
            buff.resetCount();
            int i = 0;
            for (TableFilter f : select.getTopFilters()) {
                do {
                    buff.appendExceptFirst(" ");
                    buff.append(getPlanSQL(f, i++ > 0));
                    f = f.getJoin();
                } while (f != null);
            }
        }
        Expression condition = select.getCondition();
        if (condition != null) {
            buff.append(" WHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        int[] groupIndex = select.getGroupIndex();
        if (groupIndex != null) {
            buff.append(" GROUP BY ");
            buff.resetCount();
            for (int gi : groupIndex) {
                Expression g = exprList[gi];
                g = g.getNonAliasExpression();
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        ArrayList<Expression> group = select.getGroupBy();
        if (group != null) {
            buff.append(" GROUP BY ");
            buff.resetCount();
            for (Expression g : group) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getSQL()));
            }
        }
        Expression having = select.getHaving();
        int havingIndex = select.getHavingIndex();
        if (having != null) {
            // could be set in addGlobalCondition
            // in this case the query is not run directly, just getPlanSQL is
            // called
            Expression h = having;
            buff.append(" HAVING ").append(StringUtils.unEnclose(h.getSQL()));
        } else if (havingIndex >= 0) {
            Expression h = exprList[havingIndex];
            buff.append(" HAVING ").append(StringUtils.unEnclose(h.getSQL()));
        }
        SortOrder sort = select.getSortOrder();
        if (sort != null) {
            buff.append(" ORDER BY ").append(sort.getSQL(exprList, visibleColumnCount));
        }
        Expression limitExpr = select.getLimit();
        Expression offsetExpr = select.getOffset();
        if (limitExpr != null) {
            buff.append(" LIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
            if (offsetExpr != null) {
                buff.append(" OFFSET ").append(StringUtils.unEnclose(offsetExpr.getSQL()));
            }
        }

        if (select.isForUpdate()) {
            buff.append(" FOR UPDATE");
        }
        Result result = new Result();
        result.sql = buff.toString();
        result.params = params;
        return result;
    }

    /**
     * Get the query execution plan text to use for this table filter.
     *
     * @param isJoin if this is a joined table
     * @return the SQL statement snippet
     */
    public String getPlanSQL(TableFilter filter, boolean isJoin) {
        StringBuilder buff = new StringBuilder();
        if (isJoin) {
            if (filter.isJoinOuter()) {
                buff.append("LEFT OUTER JOIN ");
            } else {
                buff.append("INNER JOIN ");
            }
        }
        if (filter.getNestedJoin() != null) {
            StringBuffer buffNested = new StringBuffer();
            TableFilter n = filter.getNestedJoin();
            do {
                buffNested.append(getPlanSQL(n, n != filter.getNestedJoin()));
                buffNested.append('\n');
                n = n.getJoin();
            } while (n != null);
            String nested = buffNested.toString();
            boolean enclose = !nested.startsWith("(");
            if (enclose) {
                buff.append("(\n");
            }
            buff.append(StringUtils.indent(nested, 4, false));
            if (enclose) {
                buff.append(')');
            }
            if (isJoin) {
                buff.append(" ON ");
                if (filter.getJoinCondition() == null) {
                    // need to have a ON expression,
                    // otherwise the nesting is unclear
                    buff.append("1=1");
                } else {
                    buff.append(StringUtils.unEnclose(filter.getJoinCondition().getSQL()));
                }
            }
            return buff.toString();
        }
        buff.append(filter.getTable().getSQL());
        if (filter.getTableAlias() != null) {
            buff.append(' ').append(Parser.quoteIdentifier(filter.getTableAlias()));
        }
        if (isJoin) {
            buff.append("\n    ON ");
            if (filter.getJoinCondition() == null) {
                // need to have a ON expression, otherwise the nesting is
                // unclear
                buff.append("1=1");
            } else {
                buff.append(StringUtils.unEnclose(filter.getJoinCondition().getSQL()));
            }
        }
        return buff.toString();
    }

}
