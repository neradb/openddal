package com.openddal.repo.mysql;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.openddal.command.CommandInterface;
import com.openddal.command.Parser;
import com.openddal.command.ddl.AlterTableAddConstraint;
import com.openddal.command.ddl.CreateIndex;
import com.openddal.command.ddl.CreateTable;
import com.openddal.command.ddl.DefineCommand;
import com.openddal.command.dml.Select;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.IndexColumn;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.engine.Database;
import com.openddal.message.DbException;
import com.openddal.repo.SQLTranslated;
import com.openddal.repo.SQLTranslator;
import com.openddal.result.SortOrder;
import com.openddal.route.rule.GroupObjectNode;
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
    public SQLTranslated translate(Select select, ObjectNode executionOn,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes) {
        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        if (executionOn instanceof GroupObjectNode) {
            return translate(select, (GroupObjectNode) executionOn, consistencyTableNodes);
        }
        Map<TableFilter, ObjectNode> nodeMapping = consistencyTableNodes.get(executionOn);
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
                buff.append(getPlanSQL(filter, i++ > 0, nodeMapping));
                filter = filter.getJoin();
            } while (filter != null);
        } else {
            buff.resetCount();
            int i = 0;
            for (TableFilter f : select.getTopFilters()) {
                do {
                    buff.appendExceptFirst(" ");
                    buff.append(getPlanSQL(f, i++ > 0, nodeMapping));
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
        return SQLTranslated.build().sql(buff.toString()).sqlParams(params);
    }

    /**
     * Get the query execution plan text to use for this table filter.
     *
     * @param isJoin if this is a joined table
     * @return the SQL statement snippet
     */
    private String getPlanSQL(TableFilter filter, boolean isJoin, Map<TableFilter, ObjectNode> nodeMapping) {
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
                buffNested.append(getPlanSQL(n, n != filter.getNestedJoin(), nodeMapping));
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
        ObjectNode tableNode = nodeMapping.get(filter);
        buff.append(identifier(tableNode.getCompositeObjectName()));
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


    @Override
    public SQLTranslated translate(Select select, GroupObjectNode node,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes) {
        ObjectNode[] items = node.getItems();
        List<Value> params = New.arrayList(10 * items.length);
        StatementBuilder sql = new StatementBuilder("SELECT * FROM (");
        for (ObjectNode objectNode : items) {
            SQLTranslated translated = translate(select, objectNode, consistencyTableNodes);
            sql.appendExceptFirst(" UNION ALL ");
            sql.append(translated.sql);
            params.addAll(translated.params);
        }
        sql.append(" ) ");
        return SQLTranslated.build().sql(sql.toString()).sqlParams(params);
    }

    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/create-table.html
     */
    @Override
    public SQLTranslated translate(CreateTable prepared, ObjectNode node, ObjectNode refNode) {
        StatementBuilder buff = new StatementBuilder("CREATE ");
        if (prepared.isTemporary()) {
            buff.append("TEMPORARY ");
        }
        buff.append("TABLE ");
        if (prepared.isIfNotExists()) {
            buff.append("IF NOT EXISTS ");
        }
        buff.append(identifier(node.getCompositeObjectName()));
        if (prepared.getComment() != null) {
            // buff.append(" COMMENT
            // ").append(StringUtils.quoteStringSQL(prepared.getComment()));
        }
        buff.append("(");
        for (Column column : prepared.getColumns()) {
            buff.appendExceptFirst(", ");
            buff.append(column.getCreateSQL());
        }
        for (DefineCommand command : prepared.getConstraintCommands()) {
            buff.appendExceptFirst(", ");
            int type = command.getType();
            switch (type) {
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY: {
                AlterTableAddConstraint stmt = (AlterTableAddConstraint) command;
                buff.append(" CONSTRAINT PRIMARY KEY");
                if (stmt.isPrimaryKeyHash()) {
                    buff.append(" USING HASH");
                }
                buff.resetCount();
                buff.append("(");
                for (IndexColumn c : stmt.getIndexColumns()) {
                    buff.appendExceptFirst(", ");
                    buff.append(identifier(c.columnName));
                }
                buff.append(")");
                break;
            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE: {
                AlterTableAddConstraint stmt = (AlterTableAddConstraint) command;
                buff.append(" CONSTRAINT UNIQUE KEY");
                buff.resetCount();
                buff.append("(");
                for (IndexColumn c : stmt.getIndexColumns()) {
                    buff.appendExceptFirst(", ");
                    buff.append(identifier(c.columnName));
                }
                buff.append(")");
                break;
            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK: {
                AlterTableAddConstraint stmt = (AlterTableAddConstraint) command;
                String enclose = StringUtils.enclose(stmt.getCheckExpression().getSQL());
                buff.append(" CHECK").append(enclose);
                break;
            }
            case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL: {
                AlterTableAddConstraint stmt = (AlterTableAddConstraint) command;
                String refTableName = refNode.getCompositeObjectName();
                IndexColumn[] cols = stmt.getIndexColumns();
                IndexColumn[] refCols = stmt.getRefIndexColumns();
                buff.resetCount();
                buff.append(" CONSTRAINT FOREIGN KEY(");
                for (IndexColumn c : cols) {
                    buff.appendExceptFirst(", ");
                    buff.append(c.columnName);
                }
                buff.append(")");
                buff.append(" REFERENCES ");
                buff.append(identifier(refTableName)).append("(");
                buff.resetCount();
                for (IndexColumn r : refCols) {
                    buff.appendExceptFirst(", ");
                    buff.append(r.columnName);
                }
                buff.append(")");
                break;
            }
            case CommandInterface.CREATE_INDEX: {
                CreateIndex stmt = (CreateIndex) command;
                if (stmt.isSpatial()) {
                    buff.append(" SPATIAL INDEX");
                } else {
                    buff.append(" INDEX");
                    if (stmt.isHash()) {
                        buff.append(" USING HASH");
                    }
                }
                buff.resetCount();
                buff.append("(");
                for (IndexColumn c : stmt.getIndexColumns()) {
                    buff.appendExceptFirst(", ");
                    buff.append(identifier(c.columnName));
                }
                buff.append(")");
                break;
            }
            default:
                throw DbException.throwInternalError("type=" + type);
            }
        }
        buff.append(")");
        if (prepared.getTableEngine() != null) {
            buff.append(" ENGINE = ");
            buff.append(prepared.getTableEngine());

        }
        ArrayList<String> tableEngineParams = prepared.getTableEngineParams();
        if (tableEngineParams != null && tableEngineParams.isEmpty()) {
            buff.append("WITH ");
            buff.resetCount();
            for (String parameter : tableEngineParams) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.quoteIdentifier(parameter));
            }
        }
        if (prepared.getCharset() != null) {
            buff.append(" DEFAULT CHARACTER SET = ");
            buff.append(prepared.getCharset());
        }
        return SQLTranslated.build().sql(buff.toString()).sql(null);

    }

}
