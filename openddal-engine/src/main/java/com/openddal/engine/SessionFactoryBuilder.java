package com.openddal.engine;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.openddal.config.Configuration;
import com.openddal.config.DataSourceProvider;
import com.openddal.config.GlobalTableRule;
import com.openddal.config.SequnceConfig;
import com.openddal.config.Shard;
import com.openddal.config.Shard.ShardItem;
import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.config.TableRuleGroup;
import com.openddal.config.parser.XmlConfigParser;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;
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
                throw new IllegalArgumentException("Can't load config file " + fromXml + " from classpath.");
            }
            XmlConfigParser parser = new XmlConfigParser(source);
            parser.parse(configuration);
        } finally {
            try {
                source.close();
            } catch (Exception e) {
                // ignored
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
        vaildateCluster(cluster);
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
        validateTableRule(group);
        List<TableRule> tableRules = group.getTableRules();
        configuration.tableRules.addAll(tableRules);
        return this;
    }

    public SessionFactoryBuilder addTableRule(ShardedTableRule table) {
        validateTableRule(table);
        configuration.tableRules.add(table);
        return this;
    }

    public SessionFactoryBuilder addTableRule(GlobalTableRule table) {
        table.config(configuration);
        validateTableRule(table);
        configuration.tableRules.add(table);
        return this;
    }

    public SessionFactoryBuilder addTableRule(TableRule table) {
        validateTableRule(table);
        configuration.tableRules.add(table);
        return this;
    }

    public SessionFactoryBuilder addSequnce(SequnceConfig sequnce) {
        validateTableRule(sequnce);
        configuration.sequnces.add(sequnce);
        return this;
    }

    public SessionFactoryBuilder defaultShardName(String shardName) {
        configuration.defaultShardName = shardName;
        return this;
    }

    public SessionFactoryBuilder forceLoadTableMate(boolean force) {
        configuration.forceLoadTableMate = force;
        return this;
    }

    public SessionFactory build() {
        beforeBuild();
        System.getProperties().putAll(configuration.settings);
        Database database = new Database(configuration);
        Engine engine = new Engine(database);
        return engine;
    }

    private void validateTableRule(TableRule oject) {
        if (oject == null) {
            throw new IllegalArgumentException();
        }
        if (StringUtils.isNullOrEmpty(oject.getName())) {
            throw new IllegalArgumentException();
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
        if (oject instanceof GlobalTableRule) {
            GlobalTableRule multiNodeObject = (GlobalTableRule) oject;
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

    private void validateTableRule(TableRuleGroup tableGroup) {
        List<TableRule> tables = tableGroup.getTableRules();
        if (tables == null || tables.isEmpty()) {
            throw new IllegalArgumentException();
        }
        int columns = -1;
        Set<String> typeFilter = New.hashSet();
        for (TableRule tableRule : tables) {
            if (tableRule == null) {
                throw new IllegalArgumentException();
            }
            typeFilter.add(typeFilter.getClass().getName());
            if (StringUtils.isNullOrEmpty(tableRule.getName())) {
                throw new IllegalArgumentException();
            }
            tableGroup.isUseTableRuleColumns();
            if (tableRule instanceof ShardedTableRule) {
                ShardedTableRule shardedTableRule = (ShardedTableRule) tableRule;
                List<String> ruleColumns = shardedTableRule.getRuleColumns();
                if (ruleColumns == null || ruleColumns.isEmpty()) {
                    throw new IllegalArgumentException("The TableRule columns is required if use table rule columns");
                }
                if (columns == -1) {
                    columns = ruleColumns.size();
                } else if (columns != ruleColumns.size()) {
                    throw new IllegalArgumentException("The length of TableRule columns must be equal");
                }
            }
        }
        if (typeFilter.size() > 1) {
            throw new IllegalArgumentException("The type of TableRule in table group must be same.");
        }

    }

    private void vaildateCluster(List<Shard> cluster) {
        if (cluster == null || cluster.isEmpty()) {
            throw new IllegalArgumentException();
        }
        for (Shard item : cluster) {
            if (StringUtils.isNullOrEmpty(item.getName())) {
                throw new IllegalArgumentException("Shard's name required.");
            }
            List<ShardItem> shardItems = item.getShardItems();
            for (ShardItem shardItem : shardItems) {
                if (StringUtils.isNullOrEmpty(shardItem.getRef())) {
                    throw new IllegalArgumentException("member 's ref is required.");
                }
                if (shardItem.getwWeight() <= 0 && shardItem.getrWeight() <= 0) {
                    throw new IllegalArgumentException("member 's weight not be less than zero.");
                }
                if (shardItem.getwWeight() <= 0) {
                    shardItem.setReadOnly(true);
                }
            }
        }
    }

    private void beforeBuild() {
        vaildateCluster(configuration.cluster);
        for (TableRule table : configuration.tableRules) {
            if (table instanceof ShardedTableRule)
                validateTableRule((ShardedTableRule) table);
            else if (table instanceof GlobalTableRule)
                validateTableRule((GlobalTableRule) table);
            else
                validateTableRule(table);
        }
        if (configuration.provider == null) {
            throw new IllegalArgumentException("DataSourceProvider is required.");
        }
    }

}
