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
// Created on 2015年1月14日
// $Id$

package com.openddal.route.rule;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import com.openddal.util.New;

/**
 * @author jorgie.li
 */
public class RoutingResult implements Comparable<RoutingResult>, Serializable {

    private static final long serialVersionUID = 1L;

    private List<ObjectNode> all;

    private List<ObjectNode> selected;

    RoutingResult(List<ObjectNode> all, List<ObjectNode> selected) {
        if (all == null || selected == null) {
            throw new IllegalArgumentException();
        }
        if (selected.size() > all.size()) {
            throw new IllegalArgumentException();
        }
        this.all = selected;
        this.selected = selected;
    }

    public static RoutingResult emptyResult() {
        List<ObjectNode> nodes = New.arrayList(0);
        return new RoutingResult(nodes, nodes);
    }

    public static RoutingResult fixedResult(List<ObjectNode> nodes) {
        return new RoutingResult(nodes, nodes);
    }

    public static RoutingResult fixedResult(ObjectNode ... tableNode) {
        List<ObjectNode> nodes = Arrays.asList(tableNode);
        return new RoutingResult(nodes, nodes);
    }

    public boolean isMultipleNode() {
        return selected.size() > 1;
    }

    public ObjectNode getSingleResult() {
        if (isMultipleNode()) {
            throw new IllegalStateException("The RoutingResult has multiple table node.");
        }
        return selected.get(0);
    }

    public ObjectNode[] getSelectNodes() {
        return selected.toArray(new ObjectNode[selected.size()]);
    }

    public boolean isFullNode() {
        return all.equals(selected) && all.size() > 1;
    }

    public int tableNodeCount() {
        return selected.size();
    }

    public ObjectNode[] group() {
        List<ObjectNode> result;
        if (isMultipleNode()) {
            Set<String> shards = shardNames();
            result = New.arrayList(shards.size());
            for (String shardName : shardNames()) {
                List<ObjectNode> groupNodes = New.arrayList(10);
                List<String> tables = New.arrayList();
                List<String> suffixes = New.arrayList();
                for (ObjectNode tableNode : selected) {
                    String nodeName = tableNode.getShardName();
                    String tableName = tableNode.getObjectName();
                    String suffix = tableNode.getSuffix();
                    if (shardName.equals(nodeName)) {
                        tables.add(tableName);
                        suffixes.add(suffix);
                        groupNodes.add(tableNode);
                    }
                }
                ObjectNode tableNode;
                if (groupNodes.size() > 1) {
                    String[] t = tables.toArray(new String[tables.size()]);
                    String[] s = suffixes.toArray(new String[suffixes.size()]);
                    ObjectNode[] items = groupNodes.toArray(new ObjectNode[groupNodes.size()]);
                    GroupObjectNode groupNode = new GroupObjectNode(shardName, items, t, s);
                    validateGroupNodeItem(groupNode);
                    tableNode = groupNode;
                } else {
                    tableNode = groupNodes.iterator().next();
                }
                result.add(tableNode);
            }
        } else {
            result = selected;
        }
        return result.toArray(new ObjectNode[result.size()]);
    }

    private Set<String> shardNames() {
        Set<String> shards = New.linkedHashSet();
        for (ObjectNode tableNode : selected) {
            String shardName = tableNode.getShardName();
            shards.add(shardName);
        }
        return shards;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((selected == null) ? 0 : selected.hashCode());
        return result;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        RoutingResult other = (RoutingResult) obj;
        if (selected == null) {
            if (other.selected != null)
                return false;
        } else if (!selected.equals(other.selected))
            return false;
        return true;
    }

    @Override
    public int compareTo(RoutingResult o) {
        Set<String> shards1 = shardNames();
        Set<String> shards2 = o.shardNames();
        int len1 = shards1.size();
        int len2 = shards2.size();
        if(len1 == len2) {
            len1 = selected.size();
            len2 = o.selected.size();
        }
        return len1 - len2;    
    }

    private void validateGroupNodeItem(GroupObjectNode node) {
        Set<String> catalog = New.hashSet();
        Set<String> schema = New.hashSet();
        for (ObjectNode objectNode : node.getItems()) {
            catalog.add(objectNode.getCatalog());
            schema.add(objectNode.getSchema());
        }
        if (catalog.size() > 1) {
            throw new IllegalStateException("Inconsistent object node catalog " + catalog);
        }
        if (schema.size() > 1) {
            throw new IllegalStateException("Inconsistent object node schema " + schema);
        }
        node.setCatalog(catalog.iterator().next());
        node.setSchema(schema.iterator().next());
    }

}
