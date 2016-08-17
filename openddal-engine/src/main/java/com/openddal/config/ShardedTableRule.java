package com.openddal.config;

import java.io.Serializable;
import java.util.Random;

import com.openddal.route.algorithm.MultColumnPartitioner;
import com.openddal.route.algorithm.Partitioner;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.StringUtils;

/**
 * @author jorgie.li
 */
public class ShardedTableRule extends TableRule implements Serializable {

    public static final int SCANLEVEL_UNLIMITED = 1;
    public static final int SCANLEVEL_ANYINDEX = 2;
    public static final int SCANLEVEL_UNIQUEINDEX = 3;
    public static final int SCANLEVEL_SHARDINGKEY = 4;

    private static final long serialVersionUID = 1L;
    private int scanLevel = SCANLEVEL_ANYINDEX;
    private Random random = new Random();
    private ObjectNode[] objectNodes;
    private String[] ruleColumns;
    private Object partitioner; 
    private TableRuleGroup ownerGroup;


    public ShardedTableRule(String name) {
        super(name);
    }

    public ShardedTableRule(String name, ObjectNode metadataNode, ObjectNode[] objectNodes) {
        super(name, metadataNode);
        this.objectNodes = objectNodes;
    }
    
    
    public int getScanLevel() {
        return scanLevel;
    }
    public void setScanLevel(int scanLevel) {
        this.scanLevel = scanLevel;
    }

    public String[] getRuleColumns() {
        return ruleColumns;
    }

    public void setRuleColumns(String[] ruleColumns) {
        this.ruleColumns = ruleColumns;
    }

    public Object getPartitioner() {
        return partitioner;
    }
    public void setPartitioner(Object partitioner) {
        if(partitioner instanceof Partitioner 
                || partitioner instanceof MultColumnPartitioner) {
            this.partitioner = partitioner;
        } else {
            throw new IllegalArgumentException();
        }
    }
    
    @Override
    public ObjectNode getMetadataNode() {
        if (this.metadataNode == null) {
            ObjectNode[] objectNodes = getObjectNodes();
            int bound = objectNodes.length - 1;
            int index = bound > 0 ? random.nextInt(bound) : bound;
            return objectNodes[index];
        }
        return this.metadataNode;
    }
    
    public ObjectNode[] getObjectNodes() {
        return objectNodes;
    }

    public void setObjectNodes(ObjectNode... objectNode) {
        for (ObjectNode item : objectNode) {
            if (StringUtils.isNullOrEmpty(item.getShardName())) {
                throw new IllegalArgumentException("The shardName attribute of ObjectNode is required.");
            }
            if (StringUtils.isNullOrEmpty(item.getObjectName())) {
                item.setObjectName(getName());
            }

        }
        this.objectNodes = objectNode;
    }

    public void cloneObjectNodes(ObjectNode... objectNode) {
        this.objectNodes = new ObjectNode[objectNode.length];
        for (int i = 0; i < objectNode.length; i++) {
            ObjectNode item = objectNode[i];
            if (StringUtils.isNullOrEmpty(item.getShardName())) {
                throw new IllegalArgumentException("The shardName attribute of ObjectNode is required.");
            }
            this.objectNodes[i] = new ObjectNode(item.getShardName(), item.getCatalog(), item.getSchema(),
                    this.getName(), item.getSuffix());
        }
    }
    
    public void cloneMetadataNode(ObjectNode node) {
        if (StringUtils.isNullOrEmpty(node.getShardName())) {
            throw new IllegalArgumentException();
        }
        this.metadataNode = new ObjectNode(node.getShardName(), node.getCatalog(), node.getSchema(), this.getName(),
                node.getSuffix());

    }
    
    public ShardedTableRule ruleColumn(String ... ruleColumn) {
        setRuleColumns(ruleColumn);
        return this;
    }
    public ShardedTableRule partitioner(Partitioner partitioner) {
        setPartitioner(partitioner);
        return this;
    }
    
    @Override
    public boolean isNodeComparable(TableRule o) {
        if (o instanceof ShardedTableRule) {
            ShardedTableRule other = (ShardedTableRule)o;
            return getOwnerGroup() == other.getOwnerGroup();
        } 
        return o.isNodeComparable(this);
    }
    
    public int getType() {
        return SHARDED_NODE_TABLE;
    }
    
    public TableRuleGroup getOwnerGroup() {
        return ownerGroup;
    }

    public void setOwnerGroup(TableRuleGroup ownerGroup) {
        this.ownerGroup = ownerGroup;
    }
}
