package com.openddal.config;

import java.util.Random;

import com.openddal.route.rule.ObjectNode;

public class GlobalTableRule extends TableRule {
    private static final long serialVersionUID = 1L;
    private Configuration configuration;
    private Random random = new Random();

    public GlobalTableRule(String name) {
        super(name, null);
    }

    public GlobalTableRule(String name, ObjectNode metadataNode) {
        super(name, metadataNode);
    }

    public ObjectNode[] getObjectNodes() {
        int size = configuration.cluster.size();
        ObjectNode[] objectNodes = new ObjectNode[size];
        for (int i = 0; i < size; i++) {
            Shard shard = configuration.cluster.get(i);
            objectNodes[i] = new ObjectNode(shard.getName(), getName());
        }
        return objectNodes;
    }
    
    public ObjectNode getMetadataNode() {
        if(this.metadataNode == null) {
            this.metadataNode = randomMetadataNode();
        }
        return this.metadataNode;
    }

    private ObjectNode randomMetadataNode() {
        ObjectNode[] objectNodes = getObjectNodes();
        int bound = objectNodes.length - 1;
        return objectNodes[random.nextInt(bound)];
    }
    
    public int getType() {
        return GLOBAL_NODE_TABLE;
    }
    
    @Override
    public boolean isNodeComparable(TableRule o) {
        return true;
    }
    
    public void config(Configuration configuration) {
        this.configuration = configuration;
    }
}
