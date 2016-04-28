package com.openddal.config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.openddal.route.algorithm.Partitioner;
import com.openddal.route.rule.ObjectNode;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class ShardedTableRule extends MultiNodeTableRule implements Serializable {

    public static final int SCANLEVEL_UNLIMITED = 1;
    public static final int SCANLEVEL_FILTER = 2;
    public static final int SCANLEVEL_ANYINDEX = 3;
    public static final int SCANLEVEL_UNIQUEINDEX = 4;
    public static final int SCANLEVEL_SHARDINGKEY = 5;

    private static final long serialVersionUID = 1L;
    private int scanLevel = SCANLEVEL_ANYINDEX;
    private List<String> ruleColumns;
    private Partitioner partitioner;
    private TableRuleGroup owner;
    
    
    public ShardedTableRule(String name) {
        super(name);
    }

    public ShardedTableRule(String name, ObjectNode metadataNode, ObjectNode[] objectNodes) {
        super(name, metadataNode, objectNodes);
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
    public ShardedTableRule ruleColumn(String ... ruleColumn) {
        setRuleColumns(Arrays.asList(ruleColumn));
        return this;
    }
    public ShardedTableRule partitioner(Partitioner partitioner) {
        setPartitioner(partitioner);
        return this;
    }
    public TableRuleGroup getOwner() {
        return owner;
    }
    void setOwner(TableRuleGroup owner) {
        this.owner = owner;
    }
    
    
    
    

}
