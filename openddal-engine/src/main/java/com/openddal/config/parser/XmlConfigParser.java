/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the “License”);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.config.parser;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.openddal.config.Configuration;
import com.openddal.config.DataSourceException;
import com.openddal.config.DefaultDataSourceProvider;
import com.openddal.config.GlobalTableRule;
import com.openddal.config.SequenceRule;
import com.openddal.config.Shard;
import com.openddal.config.Shard.ShardItem;
import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.config.TableRuleGroup;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.New;
import com.openddal.util.StringUtils;

public class XmlConfigParser {

    private XPathParser parser;

    private Configuration configuration;
    private Map<String, RuleAlgorithmConfig> algorits = New.hashMap();

    public XmlConfigParser(InputStream inputStream) {
        this(new XPathParser(inputStream, true, new ConfigEntityResolver()));
    }

    public XmlConfigParser(XPathParser parser) {
        this.configuration = new Configuration();
        this.parser = parser;
    }

    /**
     * @param ruleAlgorithm
     * @param pd
     * @param propertyValue
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    static void setPropertyWithAutomaticType(Object ruleAlgorithm, PropertyDescriptor pd, String propertyValue)
            throws IllegalAccessException, InvocationTargetException {
        Class<?> pType = pd.getPropertyType();
        if (pType == Short.class || pType == short.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Short.valueOf(propertyValue));
        } else if (pType == Byte.class || pType == byte.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Byte.valueOf(propertyValue));
        } else if (pType == Integer.class || pType == int.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Integer.valueOf(propertyValue));
        } else if (pType == Double.class || pType == double.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Double.valueOf(propertyValue));
        } else if (pType == Float.class || pType == float.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Float.valueOf(propertyValue));
        } else if (pType == Boolean.class || pType == boolean.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Boolean.valueOf(propertyValue));
        } else if (pType == Long.class || pType == long.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Long.valueOf(propertyValue));
        } else if (pType == Character.class || pType == char.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, Character.valueOf(propertyValue.toCharArray()[1]));
        } else if (pType == String.class) {
            pd.getWriteMethod().invoke(ruleAlgorithm, propertyValue);
        } else {
            throw new IllegalArgumentException("Can't set " + ruleAlgorithm.getClass().getName() + "'s property "
                    + pd.getName() + ",the type is " + pType.getName());
        }
    }

    public Configuration parse(Configuration configuration) {
        try {
            this.configuration = configuration;
            parseElement(parser.evalNode("/ddal-config"));
            return configuration;
        } catch (ParsingException e) {
            throw e;
        } catch (Exception e) {
            throw new ParsingException("Error parsing ddal-config XML . Cause: " + e, e);
        }
    }

    private void parseElement(XNode xNode) {
        parseSettings(xNode.evalNode("/ddal-config/settings"));
        parseShards(xNode.evalNodes("/ddal-config/cluster/shard"));
        parseDataSource(xNode.evalNodes("/ddal-config/dataNodes/datasource"));
        parseRuleAlgorithm(xNode.evalNodes("/ddal-config/algorithms/ruleAlgorithm"));
        parseSchemaConfig(xNode.evalNode("/ddal-config/schema"));
    }

    private void parseSettings(XNode xNode) {
        Properties prop = xNode.getChildrenAsProperties();
        configuration.settings = prop;
    }

    private void parseShards(List<XNode> xNodes) {
        List<Shard> shards = New.arrayList(xNodes.size());
        for (XNode xNode : xNodes) {
            Shard shard = new Shard();
            String name = xNode.getStringAttribute("name");
            if (StringUtils.isNullOrEmpty(name)) {
                throw new ParsingException(
                        "Error parsing ddal-config XML . Cause: element cluster.shard's name required.");
            }
            shard.setName(name);
            List<XNode> children = xNode.evalNodes("member");
            List<ShardItem> shardItems = New.arrayList(children.size());
            for (XNode child : children) {
                ShardItem shardItem = new ShardItem();
                String ref = child.getStringAttribute("ref");
                int wWeight, rWeight;
                try {
                    wWeight = child.getIntAttribute("wWeight", 1);
                    rWeight = child.getIntAttribute("rWeight", 1);
                } catch (Exception e) {
                    throw new ParsingException("incorrect wWeight or rWeight 'value for member");
                }
                if (StringUtils.isNullOrEmpty(ref)) {
                    throw new ParsingException("member 's ref is required.");
                }
                if (wWeight <= 0 && rWeight <= 0) {
                    throw new ParsingException("member 's weight not be less than zero.");
                }
                if (wWeight <= 0) {
                    shardItem.setReadOnly(true);
                }
                shardItem.setRef(ref);
                shardItem.setwWeight(wWeight);
                shardItem.setrWeight(rWeight);
                if (shardItems.contains(shardItem)) {
                    throw new ParsingException("Duplicate datasource reference in " + name);
                }
                shardItems.add(shardItem);
            }
            shard.setShardItems(shardItems);
            shards.add(shard);
        }
        configuration.cluster = shards;

    }

    private void parseDataSource(List<XNode> xNodes) {
        DefaultDataSourceProvider provider = new DefaultDataSourceProvider();
        for (XNode dataSourceNode : xNodes) {
            String id = dataSourceNode.getStringAttribute("id");

            if (!StringUtils.isNullOrEmpty(id)) {
                String jndiName = dataSourceNode.getStringAttribute("jndi-name");
                String clazz = dataSourceNode.getStringAttribute("class");
                Properties prop = dataSourceNode.getChildrenAsProperties();

                if (!StringUtils.isNullOrEmpty(jndiName)) {
                    provider.addDataNode(id, lookupJndiDataSource(jndiName, prop));
                } else if (!StringUtils.isNullOrEmpty(clazz)) {
                    provider.addDataNode(id, constructDataSource(id, clazz, prop));
                } else {
                    throw new ParsingException("datasource must be 'jndi-name' or 'class' type.");
                }
            } else {
                throw new ParsingException(
                        "Error parsing ddal-config XML . Cause: datasource attribute 'id' required.");
            }
        }
        configuration.provider = provider;
    }

    private void parseRuleAlgorithm(List<XNode> xNodes) {
        for (XNode xNode : xNodes) {
            RuleAlgorithmConfig config = new RuleAlgorithmConfig();
            String name = xNode.getStringAttribute("name");
            String clazz = xNode.getStringAttribute("class");
            if (StringUtils.isNullOrEmpty(name)) {
                throw new ParsingException("partitioner attribute 'name' is required.");
            }
            if (StringUtils.isNullOrEmpty(clazz)) {
                throw new ParsingException("partitioner attribute 'class' is required.");
            }
            config.clazz = clazz;
            config.properties = xNode.getChildrenAsProperties();
            if (algorits.get(name) != null) {
                throw new ParsingException("Duplicate ruleAlgorithm name " + name);
            }
            algorits.put(name, config);
        }
    }

    private void addTableRuleIfNotDuplicate(TableRule tableRule) {
        for (TableRule item : configuration.tableRules) {
            if (tableRule.getName().equalsIgnoreCase(item.getName())) {
                throw new ParsingException("Duplicate table name '" + tableRule.getName() + "' in schema.");
            }
        }
        configuration.tableRules.add(tableRule);
    }
    private void addSequnceConfIfNotDuplicate(SequenceRule seq) {
        for (SequenceRule item : configuration.sequnces) {
            if (seq.getName().equalsIgnoreCase(item.getName())) {
                throw new ParsingException("Duplicate sequnce name '" + seq.getName() + "' in schema.");
            }
        }
        configuration.sequnces.add(seq);
    }

    private void parseSchemaConfig(XNode xNode) {
        String name = xNode.getStringAttribute("name");
        String publicDB = xNode.getStringAttribute("public");
        boolean forceLoadTableMate = xNode.getBooleanAttribute("force", true);
        if (StringUtils.isNullOrEmpty(name)) {
            throw new ParsingException("schema attribute 'name' is required.");
        }
        configuration.publicDB = publicDB;
        configuration.forceLoadTableMate = forceLoadTableMate;

        List<XNode> xNodes = xNode.evalNodes("tableGroup");
        for (XNode tableGroupNode : xNodes) {
            parseTableGroup(tableGroupNode);
        }
        xNodes = xNode.evalNodes("table");
        for (XNode tableNode : xNodes) {
            parseTableConfig(tableNode);
        }
        
        xNodes = xNode.evalNodes("sequence");
        for (XNode tableNode : xNodes) {
            parseSequenceConfig(tableNode);
        }

    }

    /**
     * @param tableNode
     * @return
     */
    private void parseTableGroup(XNode tableNode) {
        TableRuleGroup group = new TableRuleGroup();
        ShardedTableRule templete = parseShardedTableRule(tableNode);
        group.setRuleColumns(templete.getRuleColumns());
        group.setScanLevel(templete.getScanLevel());
        group.setPartitioner(templete.getPartitioner());
        group.setObjectNodes(templete.getObjectNodes());
        group.setMetadataNode(templete.getMetadataNode());
        
        List<XNode> tableNodes = tableNode.evalNodes("tables/table");
        for (XNode xNode : tableNodes) {
            String tableName = xNode.getStringAttribute("name");
            String ruleColumns = xNode.getStringAttribute("ruleColumns", "");
            String[] columns = StringUtils.arraySplit(ruleColumns, ',', true);
            if (group.isUseTableRuleColumns() && columns.length == 0) {
                throw new ParsingException(
                        "table attribute ruleColumns is required if tableGroup use table.ruleColumns");
            }
            ShardedTableRule item = new ShardedTableRule(tableName);
            item.setRuleColumns(columns);
            group.addTableRules(item);
        }
        for (TableRule rule : group.getTableRules()) {
            addTableRuleIfNotDuplicate(rule);
        }
    }

    /**
     * @param scConfig
     * @param template
     * @param tableNode
     * @return
     */
    private void parseTableConfig(XNode tableNode) {
        String tableName = tableNode.getStringAttribute("name");
        if (StringUtils.isNullOrEmpty(tableName)) {
            throw new ParsingException("table attribute 'name' is required.");
        }

        XNode broadcast = tableNode.evalNode("broadcast");
        List<XNode> nodeNodes = tableNode.evalNodes("nodes/node");
        TableRule table;
        if (!nodeNodes.isEmpty()) {
            table = parseShardedTableRule(tableNode);
        } else if (broadcast != null) {
            table = parseGlobalTableRule(tableNode);
        } else {
            table = parseSingleNodeTableRule(tableNode);
        }
        addTableRuleIfNotDuplicate(table);
    }

    private ShardedTableRule parseShardedTableRule(XNode tableNode) {
        String tableName = tableNode.getStringAttribute("name");
        ShardedTableRule shardTable = new ShardedTableRule(tableName);
        XNode tableRule = tableNode.evalNode("tableRule");
        String metaNodeIndex = tableNode.getStringAttribute("metaNodeIndex");
        String scanLevel = tableNode.getStringAttribute("scanLevel");
        setTableScanLevel(shardTable, scanLevel);
        if(tableRule == null) {
            throw new ParsingException("tableRule is required for sharding table " + tableName);
        }
        for (XNode child : tableRule.getChildren()) {
            String name = child.getName();
            String text = getStringBody(child);
            text = text.replaceAll("\\s", "");
            if ("columns".equals(name)) {
                if (StringUtils.isNullOrEmpty(text)) {
                    throw new ParsingException("The " + tableNode.getName() + " has no rules columns defined.");
                }
                String[] columns = StringUtils.arraySplit(text, ',', true);
                shardTable.setRuleColumns(columns);
            } else if ("algorithm".equals(name)) {
                if (StringUtils.isNullOrEmpty(text)) {
                    throw new ParsingException("The " + tableNode.getName() + "  has no algorithm defined.");
                }
                RuleAlgorithmConfig algorithmConfig = algorits.get(text);
                if (algorithmConfig == null) {
                    throw new ParsingException("The algorithm ref '" + text + "' is not found.");
                }
                Object algorithm = constructAlgorithm(algorithmConfig);
                shardTable.setPartitioner(algorithm);
            }
        }
        parseNodes(shardTable, tableNode.evalNodes("nodes/node"));
        // alter object node init.
        setMetaNodeIndex(shardTable, metaNodeIndex);
        return shardTable;

    }

    private GlobalTableRule parseGlobalTableRule(XNode tableNode) {
        String tableName = tableNode.getStringAttribute("name");
        GlobalTableRule globalTableRule = new GlobalTableRule(tableName, null);
        String metaNodeIndex = tableNode.getStringAttribute("metaNodeIndex");
        List<XNode> evalNodes = tableNode.evalNodes("broadcast/node");
        if(evalNodes.isEmpty()) {
            XNode broadcast = tableNode.evalNode("broadcast");
            String text = getStringBody(broadcast);
            text = text.replaceAll("\\s", "");
            if (!StringUtils.isNullOrEmpty(text)) {
                List<String> asList = Arrays.asList(StringUtils.arraySplit(text, ',', true));
                HashSet<String> shards = new HashSet<String>(asList);
                List<ObjectNode> objectNodes = New.arrayList(shards.size());
                for (String shard : shards) {
                    objectNodes.add(new ObjectNode(shard, tableName));
                }
                globalTableRule.setBroadcasts(objectNodes.toArray(new ObjectNode[objectNodes.size()]));
            }
        } else {
            parseNodes(globalTableRule, evalNodes);
        }
        // alter object node init.
        setMetaNodeIndex(globalTableRule, metaNodeIndex);
        return globalTableRule;
    }

    private TableRule parseSingleNodeTableRule(XNode tableNode) {
        String tableName = tableNode.getStringAttribute("name");
        TableRule tableRule = new TableRule(tableName);
        XNode nodeNode = tableNode.evalNode("node");
        if (nodeNode != null) {
            String shard = nodeNode.getStringAttribute("shard");
            String catalog = nodeNode.getStringAttribute("catalog");
            String schema = nodeNode.getStringAttribute("schema");
            String name = nodeNode.getStringAttribute("name");
            shard = shard == null ? null : shard.replaceAll("\\s", "");
            if (StringUtils.isNullOrEmpty(shard)) {
                throw new ParsingException(
                        "Error parsing XML. Cause: " + "the shard attribute of 'node' element is required.");
            }
            name = StringUtils.isNullOrEmpty(name) ? tableName : name;
            tableRule.setMetadataNode(new ObjectNode(shard, catalog, schema, name, null));
        } else {
            if (StringUtils.isNullOrEmpty(configuration.publicDB)) {
                throw new ParsingException("Need config schema defaultShardName if not config table node");
            }
            tableRule.setMetadataNode(new ObjectNode(configuration.publicDB, tableName));
        }
        return tableRule;
    }

    private void parseNodes(TableRule table, List<XNode> list) {
        if (list.isEmpty()) {
            throw new ParsingException("Table 'nodes' element is required.");
        }
        List<ObjectNode> tableNodes = New.arrayList();
        for (XNode xNode : list) {
            String shard = xNode.getStringAttribute("shard");
            String catalog = xNode.getStringAttribute("catalog");
            String schema = xNode.getStringAttribute("schema");
            String name = xNode.getStringAttribute("name");
            String suffix = xNode.getStringAttribute("suffix");
            shard = shard == null ? null : shard.replaceAll("\\s", "");
            suffix = suffix == null ? null : suffix.replaceAll("\\s", "");
            if (StringUtils.isNullOrEmpty(shard)) {
                throw new ParsingException(
                        "Error parsing ddal-rule XML. Cause: " + "the shard attribute of 'node' element is required.");
            }
            name = StringUtils.isNullOrEmpty(name) ? table.getName() : name;
            List<String> shards = collectItems(shard);
            List<String> suffixes = collectItems(suffix);
            if (suffixes.isEmpty()) {
                for (String shardItem : shards) {
                    ObjectNode node = new ObjectNode(shardItem, catalog, schema, name, null);
                    if (tableNodes.contains(node)) {
                        throw new ParsingException("Duplicate " + node + " defined in " + table.getName() + "'s nodes");
                    }
                    tableNodes.add(node);
                }
            } else {
                for (String shardItem : shards) {
                    for (String suffixItem : suffixes) {
                        ObjectNode node = new ObjectNode(shardItem, catalog, schema, name, suffixItem);
                        if (tableNodes.contains(node)) {
                            throw new ParsingException(
                                    "Duplicate " + node + " defined in " + table.getName() + "'s nodes");
                        }
                        tableNodes.add(node);
                    }
                }
            }
        }
        switch (table.getType()) {
        case TableRule.GLOBAL_NODE_TABLE:
            GlobalTableRule global = (GlobalTableRule) table;
            global.setBroadcasts(tableNodes.toArray(new ObjectNode[tableNodes.size()]));
            break;
        case TableRule.SHARDED_NODE_TABLE:
            ShardedTableRule shard = (ShardedTableRule) table;
            shard.setObjectNodes(tableNodes.toArray(new ObjectNode[tableNodes.size()]));
            break;
        default:
            throw new ParsingException("parseNodes support GLOBAL_NODE_TABLE or SHARDED_NODE_TABLE");
        }
    }

    private String getStringBody(XNode xNode) {
        StringBuilder sb = new StringBuilder();
        NodeList children = xNode.getNode().getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            XNode child = xNode.newXNode(children.item(i));
            String nodeName = child.getNode().getNodeName();
            if (child.getNode().getNodeType() == Node.CDATA_SECTION_NODE
                    || child.getNode().getNodeType() == Node.TEXT_NODE) {
                String data = child.getStringBody("");
                sb.append(data);
            } else if (child.getNode().getNodeType() == Node.ELEMENT_NODE) {
                throw new ParsingException("Unknown element <" + nodeName + "> in rule XML .");
            }
        }
        String body = sb.toString();
        return body;
    }

    private List<String> collectItems(String items) {
        List<String> result = New.arrayList();
        if (StringUtils.isNullOrEmpty(items)) {
            return result;
        } else if (items.indexOf("-") != -1) {
            String[] list = StringUtils.arraySplit(items, '-', true);
            if (list.length != 2) {
                throw new ParsingException("Invalid conjunction item'" + items + "'");
            }
            throw new ParsingException("Not support - now");
        } else {
            String[] array = StringUtils.arraySplit(items, ',', true);
            result = Arrays.asList(array);
            if (result.size() != new HashSet<String>(result).size()) {
                throw new ParsingException("Duplicate item " + items);
            }
        }
        return result;
    }

    private void setTableScanLevel(ShardedTableRule table, String scanLevel) {
        int level = 0;
        if ("unlimited".equals(scanLevel)) {
            level = ShardedTableRule.SCANLEVEL_UNLIMITED;
        } else if ("anyIndex".equals(scanLevel)) {
            level = ShardedTableRule.SCANLEVEL_ANYINDEX;
        } else if ("uniqueIndex".equals(scanLevel)) {
            level = ShardedTableRule.SCANLEVEL_UNIQUEINDEX;
        } else if ("shardingKey".equals(scanLevel)) {
            level = ShardedTableRule.SCANLEVEL_SHARDINGKEY;
        }
        if (level > 0) {
            table.setScanLevel(level);
        }
    }

    private void setMetaNodeIndex(TableRule table, String metaNodeIndex) {
        if (StringUtils.isNullOrEmpty(metaNodeIndex)) {
            return;
        }
        int index = 0;
        try {
            index = Integer.parseInt(metaNodeIndex);
            if (table.getType() == TableRule.GLOBAL_NODE_TABLE) {
                ShardedTableRule tr = (ShardedTableRule) table;
                ObjectNode metaNode = tr.getObjectNodes()[index];
                table.setMetadataNode(metaNode);
            } else if (table.getType() == TableRule.SHARDED_NODE_TABLE) {
                GlobalTableRule tr = (GlobalTableRule) table;
                ObjectNode metaNode = tr.getBroadcasts()[index];
                table.setMetadataNode(metaNode);
            }
        } catch (NumberFormatException e) {
            throw new ParsingException("table 's attribute metaNodeIndex must be integer.");
        } catch (IndexOutOfBoundsException e) {
            throw new ParsingException("table metaNodeIndex out of bounds " + index + "nodes array ");
        }
    }

    private DataSource constructDataSource(String id, String clazz, Properties prop) {
        try {
            DataSource dataSource = (DataSource) Class.forName(clazz).newInstance();
            BeanInfo beanInfo = Introspector.getBeanInfo(dataSource.getClass());
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                String propertyValue = prop.getProperty(propertyDescriptor.getName());
                if (propertyValue != null) {
                    setPropertyWithAutomaticType(dataSource, propertyDescriptor, propertyValue);
                }
            }
            return dataSource;
        } catch (InstantiationException e) {
            throw new DataSourceException("There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        } catch (IllegalAccessException e) {
            throw new DataSourceException("There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        } catch (ClassNotFoundException e) {
            throw new DataSourceException("There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        } catch (Exception e) {
            throw new DataSourceException("There was an error to construct DataSource id = " + id + ". Cause: " + e, e);
        }

    }

    private DataSource lookupJndiDataSource(String jndiName, Properties jndiContext) {
        DataSource dataSource = null;
        try {
            InitialContext initCtx = null;
            initCtx = new InitialContext(jndiContext);
            dataSource = (DataSource) initCtx.lookup(jndiName);
            return dataSource;
        } catch (NamingException e) {
            throw new DataSourceException("There was an error configuring JndiDataSource.", e);
        } catch (Exception e) {
            throw new DataSourceException("There was an error configuring JndiDataSource.", e);
        }
    }

    private Object constructAlgorithm(RuleAlgorithmConfig algorithmConfig) {
        try {
            Object object = Class.forName(algorithmConfig.clazz).newInstance();
            Class<?> objectClass = object.getClass();
            BeanInfo beanInfo = Introspector.getBeanInfo(objectClass);
            PropertyDescriptor[] propertyDescriptors = beanInfo.getPropertyDescriptors();
            for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
                String propertyValue = algorithmConfig.properties.getProperty(propertyDescriptor.getName());
                if (propertyValue != null) {
                    XmlConfigParser.setPropertyWithAutomaticType(object, propertyDescriptor, propertyValue);
                }
            }
            return object;
        } catch (InvocationTargetException e) {
            throw new ParsingException("There was an error to construct partitioner " + algorithmConfig.clazz
                    + " Cause: " + e.getTargetException(), e);
        } catch (Exception e) {
            throw new ParsingException(
                    "There was an error to construct partitioner " + algorithmConfig.clazz + " Cause: " + e, e);
        }
    }

    private class RuleAlgorithmConfig {
        private String clazz;
        private Properties properties;
    }
    
    
    private void parseSequenceConfig(XNode tableNode) {
        String name = tableNode.getStringAttribute("name");
        String strategy = tableNode.getStringAttribute("strategy");
        if (StringUtils.isNullOrEmpty(name)) {
            throw new ParsingException("Sequence attribute 'name' is required.");
        }
        Properties prop = tableNode.getChildrenAsProperties();
        String shardName = prop.getProperty("shard");
        if ("hilo".equals(strategy) 
                && StringUtils.isNullOrEmpty(configuration.publicDB) 
                && StringUtils.isNullOrEmpty(shardName)) {
            throw new ParsingException("Hilo sequence " + name
                    + " requires 'shard' property if no public shard.");
        }
        SequenceRule seq = new SequenceRule(name);
        seq.setStrategy(strategy);
        seq.setProperties(prop);
        addSequnceConfIfNotDuplicate(seq);
    }

}
