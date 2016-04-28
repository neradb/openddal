package com.openddal.config;

import java.io.Serializable;

import com.openddal.route.rule.ObjectNode;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class TableRule implements Serializable {
    private static final long serialVersionUID = 1L;
    private String name;
    private ObjectNode metadataNode;
    private boolean validation = true;
    private TableRuleGroup ownerGroup;

    public TableRule() {

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

    public void setName(String name) {
        this.name = name;
    }

    public void setMetadataNode(ObjectNode metadataNode) {
        this.metadataNode = metadataNode;
    }

    public TableRule newFixedNodeTable(String name, ObjectNode metadataNode) {
        return new TableRule(name, metadataNode);
    }

    public TableRule newFixedNodeIndex(String name, ObjectNode metadataNode) {
        return new TableRule(name, metadataNode);
    }
    
    public MultiNodeTableRule newMultiNodeTable(String name, ObjectNode metadataNode, ObjectNode[] objectNodes) {
        return new MultiNodeTableRule(name, metadataNode, objectNodes);
    }
    
    public MultiNodeTableRule newMultiNodeIndex(String name, ObjectNode metadataNode, ObjectNode[] objectNodes) {
        return new MultiNodeTableRule(name, metadataNode,objectNodes);
    }
    
    public ShardedTableRule newShardedTable(String name, ObjectNode metadataNode, ObjectNode[] objectNodes) {
        return new ShardedTableRule(name, metadataNode, objectNodes);
    }
    
    public boolean isValidation() {
        return validation;
    }
    
    public void setValidation(boolean validation) {
        this.validation = validation;
    }

    public TableRuleGroup getOwnerGroup() {
        return ownerGroup;
    }

    protected void setOwnerGroup(TableRuleGroup ownerGroup) {
        this.ownerGroup = ownerGroup;
    }
    
    

}
