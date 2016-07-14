package com.openddal.test.utils;

import java.util.List;
import java.util.Map;

import com.openddal.route.rule.GroupObjectNode;
import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingArgument;
import com.openddal.route.rule.RoutingResult;
import com.openddal.route.rule.RuleEvaluateException;
import com.openddal.util.New;
import com.openddal.value.Value;

public class MultColumnPartitioner implements com.openddal.route.algorithm.MultColumnPartitioner {

    private ObjectNode[] group;
    private Map<ObjectNode,Integer> indexMapping = New.hashMap();
    private ModePartitioner forShard;
    private ModePartitioner forTable;
    private int dbCount;
    private int tblCount;

    @Override
    public Integer[] partition(List<RoutingArgument> args) {
        if (args.size() != 2) {
            throw new RuleEvaluateException("impossibility");
        }
        Integer[] shardSelect, tableSelect;
        RoutingArgument dbArg = args.get(0);
        switch (dbArg.getArgumentType()) {
        case RoutingArgument.FIXED_ROUTING_ARGUMENT:
            shardSelect = forShard.partition(dbArg.getValues().toArray(new Value[dbArg.getValues().size()]));
        case RoutingArgument.RANGE_ROUTING_ARGUMENT:
            Value start = dbArg.getStart();
            Value end = dbArg.getEnd();
            shardSelect = forShard.partition(start, end);
        default:
            shardSelect = new Integer[] { 0, 1, 2, 3 };
        }
        RoutingArgument tableArg = args.get(0);
        switch (tableArg.getArgumentType()) {
        case RoutingArgument.FIXED_ROUTING_ARGUMENT:
            tableSelect = forTable.partition(tableArg.getValues().toArray(new Value[tableArg.getValues().size()]));
        case RoutingArgument.RANGE_ROUTING_ARGUMENT:
            Value start = tableArg.getStart();
            Value end = tableArg.getEnd();
            tableSelect = forTable.partition(start, end);
        default:
            tableSelect = new Integer[] { 0, 1, 2, 3 };
        }
        List<ObjectNode> selectNode = New.arrayList(10);
        for (Integer dbindex : shardSelect) {
            GroupObjectNode dbnode = (GroupObjectNode)group[dbindex];
            for (Integer tblindex : tableSelect) {
                selectNode.add(dbnode.getItems()[tblindex]);
            }
        }
        return toIndex(selectNode);
    }

    private Integer[] toIndex(List<ObjectNode> selectNode) {
        List<Integer> result = New.arrayList(10);
        for (ObjectNode i : selectNode) {
            Integer integer = indexMapping.get(i);
            result.add(integer);
        }
        return result.toArray(new Integer[result.size()]);
    }

    @Override
    public void initialize(ObjectNode[] tableNodes) {
        for (int i = 0; i < tableNodes.length; i++) {
            indexMapping.put(tableNodes[i], i);
        }
        RoutingResult fixedResult = RoutingResult.fixedResult(tableNodes);
        this.group = fixedResult.group();
        forShard = new ModePartitioner();
        forShard.initialize(new ObjectNode[dbCount]);
        forTable = new ModePartitioner();
        forTable.initialize(new ObjectNode[tblCount]);

    }

    public void setDbCount(int dbCount) {
        this.dbCount = dbCount;
    }

    public void setTblCount(int tblCount) {
        this.tblCount = tblCount;
    }

    
    
}
