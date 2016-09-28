package com.openddal.executor.works;

import java.util.Map;
import java.util.Set;

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
import com.openddal.engine.Session;
import com.openddal.result.Row;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;

/**
 * @author jorgie.li
 */
public class WorkerFactoryProxy implements WorkerFactory {

    private final WorkerFactory target;
    private final Set<Worker> workerHolder = New.hashSet();

    public WorkerFactoryProxy(Session session) {
        this.target = session.getDatabase().getRepository().getWorkerFactory();
    }

    @Override
    public QueryWorker createQueryWorker(Select select, ObjectNode node,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes,Expression[] rewriteCols, Integer limit, Integer offset) {
        QueryWorker handler = target.createQueryWorker(select, node, consistencyTableNodes,rewriteCols, limit,offset);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public QueryWorker createQueryWorker(Column[] searchColumns, TableFilter filter, ObjectNode node) {
        QueryWorker handler = target.createQueryWorker(searchColumns, filter, node);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public QueryWorker createQueryWorker(Call call, ObjectNode node) {
        QueryWorker handler = target.createQueryWorker(call, node);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Insert insert, ObjectNode node, Row ... rows) {
        UpdateWorker handler = target.createUpdateWorker(insert, node, rows);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Update update, ObjectNode node, Row row) {
        UpdateWorker handler = target.createUpdateWorker(update, node, row);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Delete delete, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(delete, node);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Replace replace, ObjectNode node, Row ... rows) {
        UpdateWorker handler = target.createUpdateWorker(replace, node, rows);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Merge merge, ObjectNode node, Row ... rows) {
        UpdateWorker handler = target.createUpdateWorker(merge, node, rows);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Call call, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(call, node);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(CreateTable createTable, ObjectNode node, ObjectNode refNode) {
        UpdateWorker handler = target.createUpdateWorker(createTable, node, refNode);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(DropTable dropTable, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(dropTable, node);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(TruncateTable truncateTable, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(truncateTable, node);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableAddConstraint alterTableAddConstraint, ObjectNode node, ObjectNode refNode) {
        UpdateWorker handler = target.createUpdateWorker(alterTableAddConstraint, node, refNode);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableAlterColumn alterTableAlterColumn, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(alterTableAlterColumn, node);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableRename alterTableRename, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(alterTableRename, node);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableDropConstraint alterTableRename, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(alterTableRename, node);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(CreateIndex createIndex, ObjectNode indexNode, ObjectNode tableNode) {
        UpdateWorker handler = target.createUpdateWorker(createIndex, indexNode, tableNode);
        handler = holdeWorker(handler);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(DropIndex dropIndex, ObjectNode indexNode, ObjectNode tableNode) {
        UpdateWorker handler = target.createUpdateWorker(dropIndex, indexNode, tableNode);
        handler = holdeWorker(handler);
        return handler;
    }

    public synchronized void closeWorkers() {
        for (Worker worker : workerHolder) {
            try {
                worker.close();
            } catch (Throwable e) {
                // ignored
            }
        }
        workerHolder.clear();
    }

    public synchronized void cancelWorkers() {
        for (Worker worker : workerHolder) {
            try {
                worker.cancel();
            } catch (Throwable e) {
                // ignored
            }
        }
    }

    public synchronized boolean hasHoldeWorker() {
        return !workerHolder.isEmpty();
    }
    
    private synchronized <T extends Worker> T holdeWorker(T target) {
        workerHolder.add(target);
        return target;
    }


}
