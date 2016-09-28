package com.openddal.repo;

import java.util.Map;

import com.openddal.command.ddl.AlterTableAddConstraint;
import com.openddal.command.ddl.AlterTableAlterColumn;
import com.openddal.command.ddl.AlterTableDropConstraint;
import com.openddal.command.ddl.AlterTableRename;
import com.openddal.command.ddl.CreateIndex;
import com.openddal.command.ddl.CreateTable;
import com.openddal.command.ddl.DropIndex;
import com.openddal.command.ddl.DropTable;
import com.openddal.command.ddl.TruncateTable;
import com.openddal.command.dml.Call;
import com.openddal.command.dml.Delete;
import com.openddal.command.dml.Insert;
import com.openddal.command.dml.Merge;
import com.openddal.command.dml.Replace;
import com.openddal.command.dml.Select;
import com.openddal.command.dml.Update;
import com.openddal.command.expression.Expression;
import com.openddal.dbobject.table.Column;
import com.openddal.dbobject.table.TableFilter;
import com.openddal.executor.works.QueryWorker;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.executor.works.WorkerFactory;
import com.openddal.result.Row;
import com.openddal.route.rule.ObjectNode;

public class JdbcWorkerFactory implements WorkerFactory {

    private final JdbcRepository repo;

    public JdbcWorkerFactory(JdbcRepository repo) {
        this.repo = repo;
    }

    @Override
    public QueryWorker createQueryWorker(Select select, ObjectNode node,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes, Expression[] rewriteCols,
            Integer limit, Integer offset) {
        SQLTranslated translated = repo.getSQLTranslator().translate(select, node, consistencyTableNodes, rewriteCols,
                limit, offset);
        JdbcQueryWorker handler = new JdbcQueryWorker(select.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public QueryWorker createQueryWorker(Column[] searchColumns, TableFilter filter, ObjectNode node) {
        SQLTranslated translated = repo.getSQLTranslator().translate(searchColumns, filter, node);
        JdbcQueryWorker handler = new JdbcQueryWorker(filter.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public QueryWorker createQueryWorker(Call call, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(Insert insert, ObjectNode node, Row ... rows) {
        SQLTranslated translated = repo.getSQLTranslator().translate(insert, node, rows);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(insert.getSession(), node.getShardName(), translated.sql, translated.params);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Update update, ObjectNode node, Row row) {
        SQLTranslated translated = repo.getSQLTranslator().translate(update, node, row);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(update.getSession(), node.getShardName(), translated.sql, translated.params);
        return handler;        
    
    }

    @Override
    public UpdateWorker createUpdateWorker(Delete delete, ObjectNode node) {
        SQLTranslated translated = repo.getSQLTranslator().translate(delete, node);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(delete.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Replace replace, ObjectNode node, Row ... rows) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(Merge merge, ObjectNode node, Row ... rows) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(Call call, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(CreateTable createTable, ObjectNode node, ObjectNode refNode) {
        SQLTranslated translated = repo.getSQLTranslator().translate(createTable, node, refNode);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(createTable.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(DropTable dropTable, ObjectNode node) {
        SQLTranslated translated = repo.getSQLTranslator().translate(dropTable, node);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(dropTable.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(TruncateTable prepared, ObjectNode node) {

        SQLTranslated translated = repo.getSQLTranslator().translate(prepared, node);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(prepared.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;

    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableAddConstraint prepared, ObjectNode node, ObjectNode refNode) {
        SQLTranslated translated = repo.getSQLTranslator().translate(prepared, node, refNode);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(prepared.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableAlterColumn prepared, ObjectNode node) {
        SQLTranslated translated = repo.getSQLTranslator().translate(prepared, node);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(prepared.getSession(), node.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableRename alterTableRename, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableDropConstraint alterTableRename, ObjectNode node) {
        return null;
    }

    @Override
    public UpdateWorker createUpdateWorker(CreateIndex prepared, ObjectNode indexNode, ObjectNode tableNode) {
        SQLTranslated translated = repo.getSQLTranslator().translate(prepared, indexNode, tableNode);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(prepared.getSession(), indexNode.getShardName(), translated.sql,
                translated.params);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(DropIndex prepared, ObjectNode indexNode, ObjectNode tableNode) {
        SQLTranslated translated = repo.getSQLTranslator().translate(prepared, indexNode, tableNode);
        JdbcUpdateWorker handler = new JdbcUpdateWorker(prepared.getSession(), indexNode.getShardName(), translated.sql,
                translated.params);
        return handler;
    }


}
