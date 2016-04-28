package com.openddal.config;

import com.openddal.route.rule.ObjectNode;

public class MultiNodeTableRule extends TableRule{

    private static final long serialVersionUID = 1L;
    private ObjectNode[] objectNodes;
    
    public MultiNodeTableRule() {
        super();
    }
    
    public MultiNodeTableRule(String name, ObjectNode metadataNode, ObjectNode[] objectNodes) {
        super(name, metadataNode);
        this.objectNodes = objectNodes;
    }
    
    public ObjectNode[] getObjectNodes() {
        return objectNodes;
    }
    public void setObjectNodes(ObjectNode[] objectNodes) {
        this.objectNodes = objectNodes;
    }
    
    

    
    
}
