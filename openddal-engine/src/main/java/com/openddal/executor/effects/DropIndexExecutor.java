package com.openddal.executor.effects;

import java.util.List;

import com.openddal.command.ddl.DropIndex;
import com.openddal.dbobject.table.TableMate;
import com.openddal.executor.ExecutionFramework;
import com.openddal.executor.works.UpdateWorker;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;
import com.openddal.util.New;

public class DropIndexExecutor extends ExecutionFramework {
    
    private List<UpdateWorker> workers;
    private DropIndex prepared;
    public DropIndexExecutor(DropIndex prepared) {
        this.prepared = prepared;
    }

    @Override
    protected void doPrepare() {
        String indexName = prepared.getIndexName();
        String tableName = prepared.getTableName();
        TableMate table = getTableMate(tableName);
        RoutingResult rr = routingHandler.doRoute(table);
        ObjectNode[] selectNodes = rr.getSelectNodes();
        workers = New.arrayList(selectNodes.length);
        for (ObjectNode tableNode : selectNodes) {
            ObjectNode indexNode = new ObjectNode(tableNode.getShardName(), tableNode.getCatalog(),
                    tableNode.getSchema(), indexName, tableNode.getSuffix());
            UpdateWorker worker = queryHandlerFactory.createUpdateWorker(prepared, indexNode, tableNode);
            workers.add(worker);
        }
    }

    @Override
    public int doUpdate() {
        String tableName = prepared.getTableName();
        TableMate table = getTableMate(tableName);
        int affectRows = invokeUpdateWorker(workers);
        table.loadMataData(session);
        return affectRows;
    }


    @Override
    protected String doExplain() {
        return explainForWorker(workers);
    }

}
