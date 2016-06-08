package com.openddal.excutor.handle;

import java.util.List;
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
import com.openddal.dbobject.table.TableFilter;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class HandlerTraceProxy implements QueryHandlerFactory {

    private final QueryHandlerFactory target;
    private final List<ReadWriteHandler> createdHandlers = New.arrayList(10);

    public HandlerTraceProxy(QueryHandlerFactory target) {
        this.target = target;
    }

    @Override
    public QueryHandler createQueryHandler(Select select, ObjectNode node,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes) {
        QueryHandler handler = target.createQueryHandler(select, node, consistencyTableNodes);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public QueryHandler createQueryHandler(TableFilter filter, ObjectNode node) {
        QueryHandler handler = target.createQueryHandler(filter, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public QueryHandler createQueryHandler(Call call, ObjectNode node) {
        QueryHandler handler = target.createQueryHandler(call, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(Insert insert, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(insert, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public BatchUpdateHandler createBatchUpdateHandler(Insert insert, ObjectNode node) {
        BatchUpdateHandler handler = target.createBatchUpdateHandler(insert, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(Update update, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(update, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(Delete delete, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(delete, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(Replace replace, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(replace, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(Merge merge, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(merge, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(Call call, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(call, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(CreateTable createTable, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(createTable, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(DropTable dropTable, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(dropTable, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(TruncateTable truncateTable, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(truncateTable, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(AlterTableAddConstraint alterTableAddConstraint, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(alterTableAddConstraint, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(AlterTableAlterColumn alterTableAlterColumn, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(alterTableAlterColumn, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(AlterTableRename alterTableRename, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(alterTableRename, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(AlterTableDropConstraint alterTableRename, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(alterTableRename, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(CreateIndex createIndex, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(createIndex, node);
        createdHandlers.add(handler);
        return handler;
    }

    @Override
    public UpdateHandler createUpdateHandler(DropIndex dropIndex, ObjectNode node) {
        UpdateHandler handler = target.createUpdateHandler(dropIndex, node);
        createdHandlers.add(handler);
        return handler;
    }

    public List<ReadWriteHandler> getCreatedHandlers() {
        return createdHandlers;
    }

    public void closeAllCreatedHandlers() {
        for (ReadWriteHandler handler : createdHandlers) {
            try {
                handler.close();
            } catch (Throwable e) {

            }
        }
        createdHandlers.clear();
    }
    /*
     * public void cancelAllCreatedHandlers() { for (ReadWriteHandler handler :
     * createdHandlers) { try { handler.cancel(); } catch (Throwable e) {
     * 
     * } } createdHandlers.clear(); }
     */

    public boolean hasCreatedHandlers() {
        return !createdHandlers.isEmpty();
    }

}
