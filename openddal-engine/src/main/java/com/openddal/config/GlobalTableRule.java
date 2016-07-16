package com.openddal.config;

import java.util.Random;

import com.openddal.route.rule.ObjectNode;
import com.openddal.route.rule.RoutingResult;

public class GlobalTableRule extends TableRule {
    private static final long serialVersionUID = 1L;
    private Random random = new Random();
    private ObjectNode[] broadcasts;

    public GlobalTableRule(String name, ObjectNode[] broadcasts) {
        super(name, null);
        this.broadcasts = broadcasts;
    }

    public GlobalTableRule(String name, ObjectNode metadataNode, ObjectNode[] broadcasts) {
        super(name, metadataNode);
        this.broadcasts = broadcasts;
    }
    
    @Override
    public ObjectNode getMetadataNode() {
        if (this.metadataNode == null) {
            this.metadataNode = randomMetadataNode();
        }
        return this.metadataNode;
    }

    public ObjectNode[] getBroadcasts() {
        return broadcasts;
    }

    public void setBroadcasts(ObjectNode[] broadcasts) {
        this.broadcasts = broadcasts;
    }

    private ObjectNode randomMetadataNode() {
        ObjectNode[] objectNodes = getBroadcasts();
        int bound = objectNodes.length - 1;
        int index = bound > 0 ? random.nextInt(bound) : bound;
        return objectNodes[index];
    }

    public int getType() {
        return GLOBAL_NODE_TABLE;
    }

    @Override
    public boolean isNodeComparable(TableRule o) {
        return true;
    }

    public RoutingResult getRandomRoutingResult() {
        ObjectNode select = randomMetadataNode();
        return RoutingResult.fixedResult(select);
    }

    public RoutingResult getBroadcastsRoutingResult() {
        return RoutingResult.fixedResult(broadcasts);
    }

}
