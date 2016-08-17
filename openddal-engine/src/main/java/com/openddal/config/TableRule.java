package com.openddal.config;

import java.io.Serializable;

import com.openddal.route.rule.ObjectNode;

/**
 * @author jorgie.li
 */
public class TableRule implements Serializable {
    
    public static final int FIXED_NODE_TABLE = 1;
    public static final int GLOBAL_NODE_TABLE = 2;
    public static final int SHARDED_NODE_TABLE = 3;
    
    private static final long serialVersionUID = 1L;
    protected final String name;
    protected ObjectNode metadataNode;

    public TableRule(String name) {
        this.name = name;
    }

    public TableRule(String name, ObjectNode metadataNode) {
        super();
        this.name = name;
        this.metadataNode = metadataNode;
    }

    public ObjectNode getMetadataNode() {
        return metadataNode;
    }

    public String getName() {
        return name;
    }
    
    public int getType() {
        return FIXED_NODE_TABLE;
    }

    public void setMetadataNode(ObjectNode metadataNode) {
        this.metadataNode = metadataNode;
    }
    
    public boolean isNodeComparable(TableRule o) {
        if(o.getClass() == TableRule.class) {
            return metadataNode.getShardName().equals(o.metadataNode.getShardName());
        }
        return false;
    }

    public static TableRule newFixedNodeTable(String name, ObjectNode metadataNode) {
        return new TableRule(name, metadataNode);
    }
    
    public static GlobalTableRule newGlobalNodeTable(String name, ObjectNode metadataNode,ObjectNode[] objectNodes) {
        return new GlobalTableRule(name, metadataNode, objectNodes);
    }
    
    public static ShardedTableRule newShardedTable(String name, ObjectNode metadataNode, ObjectNode[] objectNodes) {
        return new ShardedTableRule(name, metadataNode, objectNodes);
    }

}
