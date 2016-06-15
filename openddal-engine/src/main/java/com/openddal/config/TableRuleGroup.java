package com.openddal.config;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.openddal.util.New;

public class TableRuleGroup extends ShardedTableRule implements Serializable {

    public TableRuleGroup() {
        super(null);
    }

    private static final long serialVersionUID = 1L;
    public static final String RULECOLUMNS_QUOTE = "${table.ruleColumns}";
    private List<ShardedTableRule> tableRules = New.arrayList();

    public List<ShardedTableRule> getTableRules() {
        for (ShardedTableRule tableRule : tableRules) {
            tableRule.setOwnerGroup(this);
            tableRule.setPartitioner(getPartitioner());
            if (!isUseTableRuleColumns()) {
                tableRule.setRuleColumns(getRuleColumns());
            }
            if (tableRule.getScanLevel() == 0) {
                tableRule.setScanLevel(getScanLevel());
            }
            tableRule.cloneObjectNodes(getObjectNodes());
            tableRule.cloneMetadataNode(getMetadataNode());

        }
        return tableRules;
    }


    public void addTableRules(ShardedTableRule... tableRules) {
        this.tableRules.addAll(Arrays.asList(tableRules));
    }

    public boolean isUseTableRuleColumns() {
        String[] ruleColumns = this.getRuleColumns();
        return ruleColumns.length == 1 && RULECOLUMNS_QUOTE.equals(ruleColumns[0]);
    }

}
