package com.openddal.repo.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.openddal.command.CommandInterface;
import com.openddal.command.Parser;
import com.openddal.command.ddl.AlterTableAddConstraint;
import com.openddal.command.ddl.AlterTableAlterColumn;
import com.openddal.command.ddl.CreateIndex;
import com.openddal.command.ddl.CreateTable;
import com.openddal.command.ddl.DefineCommand;
import com.openddal.command.ddl.DropIndex;
import com.openddal.command.ddl.DropTable;
import com.openddal.command.ddl.TruncateTable;
import com.openddal.command.dml.Delete;
import com.openddal.command.dml.Insert;
import com.openddal.command.dml.Merge;
import com.openddal.command.dml.Replace;
import com.openddal.command.dml.Select;
import com.openddal.command.dml.Update;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.IndexColumn;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.engine.Database;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.repo.SQLTranslated;
import com.openddal.repo.SQLTranslator;
import com.openddal.result.Row;
import com.openddal.result.SortOrder;
import com.openddal.route.rule.GroupObjectNode;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;
import com.openddal.util.StatementBuilder;
import com.openddal.util.StringUtils;
import com.openddal.value.Value;
import com.openddal.value.ValueInt;
import com.openddal.value.ValueNull;

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
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes, Expression[] selectCols, Integer limit, Integer offset) {

        if (executionOn instanceof GroupObjectNode) {
            return translate(select, (GroupObjectNode) executionOn, consistencyTableNodes, selectCols, limit, offset);
        }
        Map<TableFilter, ObjectNode> nodeMapping = consistencyTableNodes.get(executionOn);
        List<Value> params = New.arrayList(10);
        ArrayList<Expression> expressions = select.getExpressions();
        Expression[] exprList = expressions.toArray(new Expression[expressions.size()]);
        StatementBuilder buff = new StatementBuilder("SELECT");
        if (select.isDistinct()) {
            buff.append(" DISTINCT");
        }
        int visibleColumnCount = selectCols.length;
        for (int i = 0; i < visibleColumnCount; i++) {
            buff.appendExceptFirst(",");
            buff.append(' ');
            buff.append(selectCols[i].getPreparedSQL(select.getSession(), params));
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
            buff.append(" WHERE ").append(
                    StringUtils.unEnclose(condition.getPreparedSQL(select.getSession(), params)));
        }
        int[] groupIndex = select.getGroupIndex();
        if (groupIndex != null) {
            buff.append(" GROUP BY ");
            buff.resetCount();
            for (int gi : groupIndex) {
                Expression g = exprList[gi];
                g = g.getNonAliasExpression();
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getPreparedSQL(select.getSession(), params)));
            }
        }
        ArrayList<Expression> group = select.getGroupBy();
        if (group != null) {
            buff.append(" GROUP BY ");
            buff.resetCount();
            for (Expression g : group) {
                buff.appendExceptFirst(", ");
                buff.append(StringUtils.unEnclose(g.getPreparedSQL(select.getSession(), params)));
            }
        }
        /*不能下发having条件，having条件在内存里进行过滤
        Expression having = select.getHaving();
        int havingIndex = select.getHavingIndex();
        if (having != null) {
            // could be set in addGlobalCondition
            // in this case the query is not run directly, just getPlanSQL is
            // called
            Expression h = having;
            buff.append(" HAVING ").append(StringUtils.unEnclose(h.getPreparedSQL(select.getSession(), params)));
        } else if (havingIndex >= 0) {
            Expression h = exprList[havingIndex];
            buff.append(" HAVING ").append(StringUtils.unEnclose(h.getPreparedSQL(select.getSession(), params)));
        }*/
        SortOrder sort = select.getSortOrder();
        if (sort != null) {
            buff.append(" ORDER BY ").append(sort.getSQL(exprList, visibleColumnCount));
        }
        if (limit != null) {
            buff.append(" LIMIT ").append("?");
            params.add(ValueInt.get(limit));
            if (offset != null) {
                buff.append(" OFFSET ").append("?");
                params.add(ValueInt.get(offset));
            }
        }

        if (select.isForUpdate()) {
            buff.append(" FOR UPDATE");
        }
        return SQLTranslated.build().sql(buff.toString()).sqlParams(params);
    
    }

    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/select.html
     */
    @Override
    public SQLTranslated translate(Select select, ObjectNode executionOn,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes) {

        ArrayList<Expression> expressions = select.getExpressions();
        Expression[] exprList = expressions.toArray(new Expression[expressions.size()]);
       
        Expression limitExpr = select.getLimit();
        Expression offsetExpr = select.getOffset();
        Integer limit = null, offset = null;
        if (limitExpr != null) {
            limit = limitExpr.getValue(select.getSession()).getInt();
        }
        if (offsetExpr != null) {
            offset = offsetExpr.getValue(select.getSession()).getInt();
        }
        return translate(select, executionOn, consistencyTableNodes, exprList, limit, offset);
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
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes,Expression[] selectCols, Integer limit, Integer offset) {
        ObjectNode[] items = node.getItems();
        List<Value> params = New.arrayList(10 * items.length);
        StatementBuilder sql = new StatementBuilder(100 * items.length);
        for (ObjectNode objectNode : items) {
            SQLTranslated translated = translate(select, objectNode, consistencyTableNodes,selectCols, limit, offset);
            sql.appendExceptFirst(" UNION ALL ");
            sql.append(StringUtils.enclose(translated.sql));
            params.addAll(translated.params);
        }
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
        return SQLTranslated.build().sql(buff.toString());

    }

    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/alter-table.html
     */
    @Override
    public SQLTranslated translate(AlterTableAddConstraint prepared, ObjectNode node, ObjectNode refNode) {
        String forTable = node.getCompositeObjectName();
        switch (prepared.getType()) {
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_PRIMARY_KEY: {
            String keyType = AlterTableAddConstraint.PRIMARY_KEY;
            StatementBuilder buff = uniqueConstraint(prepared, node, keyType);
            return SQLTranslated.build().sql(buff.toString());

        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_UNIQUE: {
            String keyType = AlterTableAddConstraint.UNIQUE + " KEY";
            StatementBuilder buff = uniqueConstraint(prepared, node, keyType);
            return SQLTranslated.build().sql(buff.toString());

        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_CHECK: {
            /*
             * MySQL. The CHECK clause is parsed but ignored by all storage
             * engines.
             */
            StringBuilder buff = new StringBuilder("ALTER TABLE ");
            buff.append(identifier(forTable)).append(" ADD CONSTRAINT ");
            String constraintName = prepared.getConstraintName();
            if (!StringUtils.isNullOrEmpty(constraintName)) {
                buff.append(constraintName);
            }
            String enclose = StringUtils.enclose(prepared.getCheckExpression().getSQL());
            buff.append(" CHECK").append(enclose).append(" NOCHECK");
            return SQLTranslated.build().sql(buff.toString());
        }
        case CommandInterface.ALTER_TABLE_ADD_CONSTRAINT_REFERENTIAL: {
            /*
             * MySQL. The FOREIGN KEY and REFERENCES clauses are supported by
             * the InnoDB and NDB storage engines
             */
            String forRefTable = refNode.getCompositeObjectName();
            StatementBuilder buff = new StatementBuilder("ALTER TABLE ");
            buff.append(identifier(forTable)).append(" ADD CONSTRAINT ");
            String constraintName = prepared.getConstraintName();
            if (!StringUtils.isNullOrEmpty(constraintName)) {
                buff.append(constraintName);
            }
            IndexColumn[] cols = prepared.getIndexColumns();
            IndexColumn[] refCols = prepared.getRefIndexColumns();
            buff.append(" FOREIGN KEY(");
            for (IndexColumn c : cols) {
                buff.appendExceptFirst(", ");
                buff.append(c.getSQL());
            }
            buff.append(')');

            buff.append(" REFERENCES ");
            buff.append(identifier(forRefTable)).append('(');
            buff.resetCount();
            for (IndexColumn r : refCols) {
                buff.appendExceptFirst(", ");
                buff.append(r.getSQL());
            }
            buff.append(')');
            if (prepared.getDeleteAction() != AlterTableAddConstraint.RESTRICT) {
                buff.append(" ON DELETE ");
                appendAction(buff, prepared.getDeleteAction());
            }
            if (prepared.getUpdateAction() != AlterTableAddConstraint.RESTRICT) {
                buff.append(" ON UPDATE ");
                appendAction(buff, prepared.getDeleteAction());
            }
            return SQLTranslated.build().sql(buff.toString());

        }
        default:
            throw DbException.throwInternalError("type=" + prepared.getType());

        }
    }

    private StatementBuilder uniqueConstraint(AlterTableAddConstraint prepared, ObjectNode node, String keyType) {
        StatementBuilder buff = new StatementBuilder("ALTER TABLE ");
        buff.append(identifier(node.getCompositeObjectName())).append(" ADD CONSTRAINT ");
        String constraintName = prepared.getConstraintName();
        // MySQL constraintName is optional
        if (!StringUtils.isNullOrEmpty(constraintName)) {
            buff.append(constraintName);
        }
        buff.append(' ').append(keyType);
        if (prepared.isPrimaryKeyHash()) {
            buff.append(" USING ").append("HASH");
        }
        buff.append('(');
        for (IndexColumn c : prepared.getIndexColumns()) {
            buff.appendExceptFirst(", ");
            buff.append(identifier(c.column.getName()));
        }
        buff.append(')');
        return buff;
    }

    private static void appendAction(StatementBuilder buff, int action) {
        switch (action) {
        case AlterTableAddConstraint.CASCADE:
            buff.append("CASCADE");
            break;
        case AlterTableAddConstraint.SET_DEFAULT:
            buff.append("SET DEFAULT");
            break;
        case AlterTableAddConstraint.SET_NULL:
            buff.append("SET NULL");
            break;
        default:
            DbException.throwInternalError("action=" + action);
        }
    }

    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/alter-table.html
     */
    @Override
    public SQLTranslated translate(AlterTableAlterColumn prepared, ObjectNode node) {

        Column oldColumn = prepared.getOldColumn();
        String forTable = node.getCompositeObjectName();
        int type = prepared.getType();
        switch (type) {
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NOT_NULL: {
            StringBuilder buff = new StringBuilder("ALTER TABLE ");
            buff.append(identifier(forTable));
            buff.append(" CHANGE COLUMN ");
            buff.append(oldColumn.getSQL()).append(' ');
            buff.append(oldColumn.getCreateSQL());
            return SQLTranslated.build().sql(buff.toString());
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_NULL: {

            StringBuilder buff = new StringBuilder("ALTER TABLE ");
            buff.append(identifier(forTable));
            buff.append(" CHANGE COLUMN ");
            buff.append(oldColumn.getSQL()).append(' ');
            buff.append(oldColumn.getCreateSQL());
            return SQLTranslated.build().sql(buff.toString());
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_DEFAULT: {
            StringBuilder buff = new StringBuilder("ALTER TABLE ");
            buff.append(identifier(forTable));
            buff.append(" ALTER COLUMN ");
            buff.append(prepared.getOldColumn().getSQL());
            buff.append(" SET DEFAULT ");
            buff.append(prepared.getDefaultExpression().getSQL());
            return SQLTranslated.build().sql(buff.toString());
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_CHANGE_TYPE: {
            StringBuilder buff = new StringBuilder("ALTER TABLE ");
            buff.append(identifier(forTable));
            buff.append(" ALTER COLUMN ");
            buff.append(prepared.getOldColumn().getSQL());
            buff.append(" SET DEFAULT ");
            buff.append(prepared.getDefaultExpression().getSQL());
            return SQLTranslated.build().sql(buff.toString());
        }
        case CommandInterface.ALTER_TABLE_ADD_COLUMN: {
            StatementBuilder buff = new StatementBuilder("ALTER TABLE ");
            buff.append(identifier(forTable));
            ArrayList<Column> columnsToAdd = prepared.getColumnsToAdd();
            for (Column column : columnsToAdd) {
                buff.appendExceptFirst(", ");
                buff.append(" ADD COLUMN ");
                buff.append(column.getCreateSQL());
            }
            return SQLTranslated.build().sql(buff.toString());
        }
        case CommandInterface.ALTER_TABLE_DROP_COLUMN: {
            StatementBuilder buff = new StatementBuilder("ALTER TABLE ");
            buff.append(identifier(forTable));
            buff.append(" DROP COLUMN ");
            buff.append(oldColumn.getSQL());
            return SQLTranslated.build().sql(buff.toString());
        }
        case CommandInterface.ALTER_TABLE_ALTER_COLUMN_SELECTIVITY:
        default:
            throw DbException.throwInternalError("type=" + type);
        }

    }

    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/drop-table.html
     */
    @Override
    public SQLTranslated translate(DropTable prepared, ObjectNode node) {
        String forTable = node.getCompositeObjectName();
        StringBuilder sql = new StringBuilder();
        sql.append("DROP TABLE");
        if (prepared.isIfExists()) {
            sql.append(" IF EXISTS");
        }
        sql.append(" ").append(identifier(forTable));
        if (prepared.getDropAction() == AlterTableAddConstraint.CASCADE) {
            sql.append(" CASCADE");
        }
        return SQLTranslated.build().sql(sql.toString());

    }

    @Override
    public SQLTranslated translate(TruncateTable truncateTable, ObjectNode node) {
        String forTable = node.getCompositeObjectName();
        StringBuilder sql = new StringBuilder();
        sql.append("TRUNCATE TABLE ");
        sql.append(identifier(forTable));
        return SQLTranslated.build().sql(sql.toString());

    }

    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/create-index.html
     */
    @Override
    public SQLTranslated translate(CreateIndex prepared, ObjectNode indexNode, ObjectNode tableNode) {
        String forIndex = indexNode.getCompositeObjectName();
        String forTable = tableNode.getCompositeObjectName();
        StatementBuilder sql = new StatementBuilder();
        sql.append("CREATE ");
        if (prepared.isUnique()) {
            sql.append(" UNIQUE");
        } else if(prepared.isSpatial()) {
            sql.append(" SPATIAL");
        }
        sql.append(" INDEX ").append(identifier(forIndex));
        sql.append(" ON ").append(identifier(forTable));
        sql.append('(');
        for (IndexColumn c : prepared.getIndexColumns()) {
            sql.appendExceptFirst(", ");
            sql.append(identifier(c.column.getName()));
        }
        sql.append(')');
        if (prepared.isHash()) {
            sql.append(" USING HASH");
        }
        String comment = prepared.getComment();
        if(comment != null) {
            sql.append(" COMMENT ").append(StringUtils.quoteStringSQL(comment));
        }
        return SQLTranslated.build().sql(sql.toString());
    }
    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/drop-index.html
     */
    @Override
    public SQLTranslated translate(DropIndex prepared, ObjectNode indexNode, ObjectNode tableNode) {
        String forIndex = indexNode.getCompositeObjectName();
        String forTable = tableNode.getCompositeObjectName();
        StatementBuilder sql = new StatementBuilder();
        sql.append("DROP INDEX ");
        sql.append(identifier(forIndex));
        sql.append(" ON ").append(identifier(forTable));
        return SQLTranslated.build().sql(sql.toString());
    }
    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/delete.html
     */
    @Override
    public SQLTranslated translate(Delete prepared, ObjectNode node) {

        ArrayList<Value> params = New.arrayList();
        String forTable = node.getCompositeObjectName();
        Expression condition = prepared.getCondition();
        Expression limitExpr = prepared.getLimitExpr();
        StatementBuilder sql = new StatementBuilder();

        sql.append("DELETE FROM ");
        sql.append(identifier(forTable));
        if (condition != null) {
            condition.getPreparedSQL(prepared.getSession(), params);
            sql.append(" WHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        if (limitExpr != null) {
            limitExpr.getPreparedSQL(prepared.getSession(), params);
            sql.append(" LIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
        }
        return SQLTranslated.build().sql(sql.toString()).sqlParams(params);

    
    }
    
    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/insert.html
     */
    @Override
    public SQLTranslated translate(Insert insert, ObjectNode node, Row ... rows) {
        ArrayList<Value> params = New.arrayList();
        StatementBuilder sql = new StatementBuilder();
        String forTable = node.getCompositeObjectName();
        Column[] columns = insert.getColumns();
        sql.append("INSERT INTO ");
        sql.append(identifier(forTable)).append('(');
        for (Column c : insert.getColumns()) {
            sql.appendExceptFirst(", ");
            sql.append(c.getSQL());
        }
        sql.append(") VALUES ");
        appendValues(params, sql, columns, rows);
        HashMap<Column, Expression> duplicateKeyMap = insert.getDuplicateKeyAssignmentMap();
        if(duplicateKeyMap != null) {
            sql.resetCount();
            sql.append(" ON DUPLICATE KEY UPDATE ");
            for (Map.Entry<Column, Expression> item: duplicateKeyMap.entrySet()) {
                sql.appendExceptFirst(", ");
                sql.append(item.getKey().getSQL());
                sql.append(" = ");
                sql.append(item.getValue().getSQL());
            }
        }
        return SQLTranslated.build().sql(sql.toString()).sqlParams(params);
    
    }

    private void appendValues(ArrayList<Value> params, StatementBuilder sql, Column[] columns, Row... rows) {
        for (int i = 0; i < rows.length; i++) {
            Row row = rows[i];
            if(i > 0) {
                sql.append(", ");
            }
            sql.append('(');
            sql.resetCount();
            for (Column c : columns) {
                int index = c.getColumnId();
                Value v = row.getValue(index);
                sql.appendExceptFirst(", ");
                if (v == null) {
                    sql.append("DEFAULT");
                } else if (isNull(v)) {
                    sql.append("NULL");
                } else {
                    sql.append('?');
                    params.add(v);
                }
            }
            sql.append(')');
        
        }
    }
    
    protected static boolean isNull(Value v) {
        return v == null || v == ValueNull.INSTANCE;
    }

    /**
     * @see http://dev.mysql.com/doc/refman/5.7/en/replace.html
     */
    @Override
    public SQLTranslated translate(Replace replace, ObjectNode node, Row ... rows) {
        ArrayList<Value> params = New.arrayList();
        String forTable = node.getCompositeObjectName();
        Column[] columns = replace.getColumns();
        StatementBuilder sql = new StatementBuilder("REPLACE INTO ");
        sql.append(identifier(forTable)).append('(');
        for (Column c : columns) {
            sql.appendExceptFirst(", ");
            sql.append(c.getSQL());
        }
        sql.append(')');
        sql.append(") VALUES ");
        appendValues(params, sql, columns, rows);
        return SQLTranslated.build().sql(sql.toString()).sqlParams(params);
    }

    @Override
    public SQLTranslated translate(Merge merge, ObjectNode node, Row ... rows) {
        throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1,"MySQL not support merge.");
    }

    @Override
    public SQLTranslated translate(Update prepared, ObjectNode node, Row row) {
        ArrayList<Value> params = New.arrayList();
        String forTable = node.getCompositeObjectName();
        List<Column> columns = prepared.getColumns();
        Expression condition = prepared.getCondition();
        Expression limitExpr = prepared.getLimitExpr();
        StatementBuilder sql = new StatementBuilder();
        sql.append("UPDATE ");
        sql.append(identifier(forTable)).append(" SET ");
        for (int i = 0, size = columns.size(); i < size; i++) {
            Column c = columns.get(i);
            sql.appendExceptFirst(", ");
            sql.append(c.getSQL()).append(" = ");

            Value v = row.getValue(i);
            sql.appendExceptFirst(", ");
            if (v == null) {
                sql.append("DEFAULT");
            } else if (isNull(v)) {
                sql.append("NULL");
            } else {
                sql.append('?');
                params.add(v);
            }
        }
        if (condition != null) {
            condition.getPreparedSQL(prepared.getSession(), params);
            sql.append(" WHERE ").append(StringUtils.unEnclose(condition.getSQL()));
        }
        if (limitExpr != null) {
            limitExpr.getPreparedSQL(prepared.getSession(), params);
            sql.append(" LIMIT ").append(StringUtils.unEnclose(limitExpr.getSQL()));
        }
        return SQLTranslated.build().sql(sql.toString()).sqlParams(params);
    }

    @Override
    public SQLTranslated translate(Column[] searchColumns, TableFilter filter, ObjectNode node) {

        // can not use the field sqlStatement because the parameter
        // indexes may be incorrect: ? may be in fact ?2 for a subquery
        // but indexes may be set manually as well
        if (node instanceof GroupObjectNode) {
            return translate(searchColumns, filter, (GroupObjectNode) node);
        }
        List<Value> params = New.arrayList(10);
        StatementBuilder buff = new StatementBuilder("SELECT");

        int visibleColumnCount = searchColumns.length;
        for (int i = 0; i < visibleColumnCount; i++) {
            buff.appendExceptFirst(",");
            buff.append(' ');
            buff.append(identifier(searchColumns[i].getName()));
        }
        buff.append(" FROM ");
        buff.append(identifier(node.getCompositeObjectName()));
        buff.append(" AS ");
        buff.append(filter.getTableAlias());
        Expression condition = filter.getFilterCondition();
        if (condition != null) {
            buff.append(" WHERE ").append(StringUtils.unEnclose(condition.getPreparedSQL(filter.getSession(), params)));
        }
        return SQLTranslated.build().sql(buff.toString()).sqlParams(params);
    
    }
    
    
    @Override
    public SQLTranslated translate(Column[] searchColumns, TableFilter filter, GroupObjectNode node) {
        ObjectNode[] items = node.getItems();
        List<Value> params = New.arrayList(10 * items.length);
        StatementBuilder sql = new StatementBuilder(100 * items.length);
        for (ObjectNode objectNode : items) {
            SQLTranslated translated = translate(searchColumns, filter, objectNode);
            sql.appendExceptFirst(" UNION ALL ");
            sql.append(StringUtils.enclose(translated.sql));
            params.addAll(translated.params);
        }
        return SQLTranslated.build().sql(sql.toString()).sqlParams(params);
    
    }

}
