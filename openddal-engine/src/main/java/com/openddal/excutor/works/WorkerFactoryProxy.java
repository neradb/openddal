package com.openddal.excutor.works;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
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
public class WorkerFactoryProxy implements WorkerFactory {

    private final WorkerFactory target;
    private final List<Worker> pendingClose = New.linkedList();

    public WorkerFactoryProxy(WorkerFactory target) {
        this.target = target;
    }

    @Override
    public QueryWorker createQueryWorker(Select select, ObjectNode node,
            Map<ObjectNode, Map<TableFilter, ObjectNode>> consistencyTableNodes) {
        QueryWorker handler = target.createQueryWorker(select, node, consistencyTableNodes);
        handler = newWorkerProxy(handler, QueryWorker.class);
        return handler;
    }

    @Override
    public QueryWorker createQueryWorker(TableFilter filter, ObjectNode node) {
        QueryWorker handler = target.createQueryWorker(filter, node);
        handler = newWorkerProxy(handler, QueryWorker.class);
        return handler;
    }

    @Override
    public QueryWorker createQueryWorker(Call call, ObjectNode node) {
        QueryWorker handler = target.createQueryWorker(call, node);
        handler = newWorkerProxy(handler, QueryWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Insert insert, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(insert, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public BatchUpdateWorker createBatchUpdateWorker(Insert insert, ObjectNode node) {
        BatchUpdateWorker handler = target.createBatchUpdateWorker(insert, node);
        handler = newWorkerProxy(handler, BatchUpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Update update, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(update, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Delete delete, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(delete, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Replace replace, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(replace, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Merge merge, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(merge, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(Call call, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(call, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(CreateTable createTable, ObjectNode node, ObjectNode refNode) {
        UpdateWorker handler = target.createUpdateWorker(createTable, node, refNode);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(DropTable dropTable, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(dropTable, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(TruncateTable truncateTable, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(truncateTable, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableAddConstraint alterTableAddConstraint, ObjectNode node, ObjectNode refNode) {
        UpdateWorker handler = target.createUpdateWorker(alterTableAddConstraint, node, refNode);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableAlterColumn alterTableAlterColumn, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(alterTableAlterColumn, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableRename alterTableRename, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(alterTableRename, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(AlterTableDropConstraint alterTableRename, ObjectNode node) {
        UpdateWorker handler = target.createUpdateWorker(alterTableRename, node);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(CreateIndex createIndex, ObjectNode indexNode, ObjectNode tableNode) {
        UpdateWorker handler = target.createUpdateWorker(createIndex, indexNode, tableNode);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    @Override
    public UpdateWorker createUpdateWorker(DropIndex dropIndex, ObjectNode indexNode, ObjectNode tableNode) {
        UpdateWorker handler = target.createUpdateWorker(dropIndex, indexNode, tableNode);
        handler = newWorkerProxy(handler, UpdateWorker.class);
        return handler;
    }

    public synchronized void close() {
        List<Worker> workers = New.arrayList(pendingClose);
        for (Worker worker : workers) {
            try {
                worker.close();
            } catch (Throwable e) {
                // ignored
            }
        }
    }

    public synchronized void cancel() {
        List<Worker> workers = New.arrayList(pendingClose);
        for (Worker worker : workers) {
            try {
                worker.cancel();
            } catch (Throwable e) {
                // ignored
            }
        }
    }

    public synchronized boolean hasRuningWorker() {
        return !pendingClose.isEmpty();
    }

    private synchronized void removeWorker(Worker worker) {
        pendingClose.remove(worker);
    }

    private synchronized void addWorker(Worker worker) {
        pendingClose.add(worker);
    }
    
    @SuppressWarnings("unchecked")
    private <T extends Worker> T newWorkerProxy(T target, Class<T> interfaceClass) {
        InvocationHandler handler = new WorkerProxy(target);
        ClassLoader cl = Connection.class.getClassLoader();
        return (T) Proxy.newProxyInstance(cl, new Class[]{interfaceClass}, handler);
    }

    private class WorkerProxy implements InvocationHandler {
        
        private Worker target;

        private WorkerProxy(Worker target) {
            this.target = target;
        }
        
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            try {
                Object invoke = method.invoke(target, args);
                if ("close".equals(method.getName())) {
                    try {
                        invoke = method.invoke(target, args);
                    } finally {
                        removeWorker(target);
                    }
                } else if ("executeUpdate".equals(method.getName()) || "executeQuery".equals(method.getName())
                        || "executeBatchUpdate".equals(method.getName())) {
                    addWorker(target);
                    invoke = method.invoke(target, args);
                } else {
                    invoke = method.invoke(target, args);
                }
                return invoke;
            } catch (InvocationTargetException ex) {
                throw ex.getTargetException();
            }
        }

    }

}
