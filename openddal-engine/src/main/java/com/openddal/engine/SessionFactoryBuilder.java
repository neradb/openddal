package com.openddal.engine;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import com.openddal.config.Configuration;
import com.openddal.config.DataSourceProvider;
import com.openddal.config.MultiNodeTableRule;
import com.openddal.config.SequnceConfig;
import com.openddal.config.Shard;
import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.config.TableRuleGroup;
import com.openddal.config.parser.XmlConfigParser;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.StringUtils;
import com.openddal.util.Utils;

public final class SessionFactoryBuilder {

    private Configuration configuration = new Configuration();

    public static SessionFactoryBuilder newBuilder() {
        return new SessionFactoryBuilder();
    }

    public SessionFactoryBuilder fromXml(String fromXml) {

        if (StringUtils.isNullOrEmpty(fromXml)) {
            throw new IllegalArgumentException("xml config file must not be null");
        }
        InputStream source = null;
        try {
            source = Utils.getResourceAsStream(fromXml);
            if (source == null) {
                throw new IllegalArgumentException(
                        "Can't load config file " + fromXml + " from classpath.");
            }
            XmlConfigParser parser = new XmlConfigParser(source);
            parser.parse(configuration);
        } finally {
            try {
                source.close();
            } catch (Exception e) {
                //ignored
            }
        }
    
        return this;
    }

    public SessionFactoryBuilder applySettings(Properties settings) {
        if (settings == null) {
            throw new IllegalArgumentException();
        }
        configuration.settings = settings;
        return this;
    }

    public SessionFactoryBuilder dbCluster(List<Shard> cluster) {
        if (cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException();
        }
        configuration.cluster = cluster;
        return this;
    }

    public SessionFactoryBuilder dataSourceProvider(DataSourceProvider provider) {
        if (provider == null) {
            throw new IllegalArgumentException();
        }
        configuration.provider = provider;
        return this;
    }

    public SessionFactoryBuilder addTableRuleGrop(TableRuleGroup group) {
        validateNode(group);
        configuration.tableGroup.add(group);
        return this;
    }
    

    public SessionFactoryBuilder addTableRule(ShardedTableRule table) {
        validateNode(table);
        configuration.shardingTable.add(table);
        return this;
    }

    public SessionFactoryBuilder addTableRule(MultiNodeTableRule table) {
        validateNode(table);
        configuration.multiNodeTables.add(table);
        return this;
    }

    public SessionFactoryBuilder addTableRule(TableRule table) {
        validateNode(table);
        configuration.fixedNodeTables.add(table);
        return this;
    }

    public SessionFactoryBuilder addIndex(MultiNodeTableRule index) {
        validateNode(index);
        configuration.multiNodeIndexs.add(index);
        return this;
    }

    public SessionFactoryBuilder addIndex(TableRule index) {
        validateNode(index);
        configuration.fixedNodeIndexs.add(index);
        return this;
    }

    public SessionFactoryBuilder addSequnce(SequnceConfig sequnce) {
        validateNode(sequnce);
        configuration.sequnces.add(sequnce);
        return this;
    }

    public SessionFactory build() {
        
        return null;
    }

    private void validateNode(TableRule oject) {
        if (oject == null) {
            throw new IllegalArgumentException();
        }
        if (StringUtils.isNullOrEmpty(oject.getName())) {
            throw new IllegalArgumentException();
        }
        if (oject instanceof TableRuleGroup) {
            TableRuleGroup group = (TableRuleGroup) oject;
            List<TableRule> tables = group.getTableRules();
            if (tables == null || tables.isEmpty()) {
                throw new IllegalArgumentException();
            }
        }
        if (oject instanceof ShardedTableRule) {
            ShardedTableRule shardingTable = (ShardedTableRule) oject;
            List<String> ruleColumns = shardingTable.getRuleColumns();
            if (ruleColumns == null || ruleColumns.isEmpty()) {
                throw new IllegalArgumentException();
            }
            for (String string : ruleColumns) {
                if (StringUtils.isNullOrEmpty(string)) {
                    throw new IllegalArgumentException();
                }
            }
            if (shardingTable.getPartitioner() == null) {
                throw new IllegalArgumentException();
            }
        }
        if (oject instanceof MultiNodeTableRule) {
            MultiNodeTableRule multiNodeObject = (MultiNodeTableRule) oject;
            ObjectNode[] objectNodes = multiNodeObject.getObjectNodes();
            if (objectNodes == null) {
                throw new IllegalArgumentException();
            }
        }
        if (TableRule.class == oject.getClass()) {
            if (oject.getMetadataNode() == null) {
                throw new IllegalArgumentException();
            }
        }
    }

}
