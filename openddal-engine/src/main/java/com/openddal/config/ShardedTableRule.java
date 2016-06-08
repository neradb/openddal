package com.openddal.config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.openddal.route.algorithm.Partitioner;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.StringUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ShardedTableRule extends TableRule implements Serializable {

    public static final int SCANLEVEL_UNLIMITED = 1;
    public static final int SCANLEVEL_ANYINDEX = 2;
    public static final int SCANLEVEL_UNIQUEINDEX = 3;
    public static final int SCANLEVEL_SHARDINGKEY = 4;

    private static final long serialVersionUID = 1L;
    private int scanLevel = SCANLEVEL_ANYINDEX;
    private ObjectNode[] objectNodes;
    private List<String> ruleColumns;
    private Partitioner partitioner; 
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
    
    public List<String> getRuleColumns() {
        return ruleColumns;
    }
    
    public void setRuleColumns(List<String> ruleColumns) {
        this.ruleColumns = ruleColumns;
    }

    public Partitioner getPartitioner() {
        return partitioner;
    }
    public void setPartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
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
    
    public ShardedTableRule ruleColumn(String ... ruleColumn) {
        setRuleColumns(Arrays.asList(ruleColumn));
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
