package com.openddal.config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class TableRuleGroup extends ShardedTableRule implements Serializable {

    
    public TableRuleGroup() {
        super(null);
    }

    private static final long serialVersionUID = 1L;
    public static final String RULECOLUMNS_QUOTE = "${table.ruleColumns}";
    private List<TableRule> tableRules;

    public List<TableRule> getTableRules() {
        for (TableRule tableRule : tableRules) {
            tableRule.setOwnerGroup(this);
            if (tableRule instanceof ShardedTableRule) {
                ShardedTableRule shardedTableRule = (ShardedTableRule) tableRule;
                shardedTableRule.setPartitioner(getPartitioner());                
                if (!isUseTableRuleColumns()) {
                    shardedTableRule.setRuleColumns(getRuleColumns());
                }
                if (SCANLEVEL_ANYINDEX == shardedTableRule.getScanLevel()) {
                    shardedTableRule.setScanLevel(getScanLevel());
                }
            } 
            if (tableRule instanceof MultiNodeTableRule) {
                MultiNodeTableRule multiNodeTableRule = (MultiNodeTableRule) tableRule;
                multiNodeTableRule.cloneObjectNodes(getObjectNodes());
            }
        }
        return tableRules;
    }

    public void setTableRules(List<TableRule> tableRules) {
        this.tableRules = tableRules;
    }

    public void setTableRules(TableRule... tableRules) {
        this.tableRules = Arrays.asList(tableRules);
    }
    
    public boolean isUseTableRuleColumns() {
        List<String> ruleColumns = this.getRuleColumns();
        return ruleColumns.size() == 1 && RULECOLUMNS_QUOTE.equals(ruleColumns.get(0));
    }

}
