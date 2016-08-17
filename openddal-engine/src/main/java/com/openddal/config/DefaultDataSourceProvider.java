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
// Created on 2014年7月30日
// $Id$

package com.openddal.config;

import java.util.Map;

import javax.sql.DataSource;

import com.openddal.util.New;
import com.openddal.util.StringUtils;

/**
 * @author jorgie.li
 */
public class DefaultDataSourceProvider implements DataSourceProvider {

    private final Map<String, DataSource> dataNodes;
    
    public DefaultDataSourceProvider() {
       this.dataNodes = New.hashMap();
    }

    public DefaultDataSourceProvider(Map<String, DataSource> dataNodes) {
        super();
        this.dataNodes = dataNodes;
    }

    /**
     * @return the dataNodes
     */
    public Map<String, DataSource> getDataNodes() {
        return dataNodes;
    }

    public void addDataNode(String id, DataSource dataSource) {
        if (dataNodes.containsKey(id)) {
            throw new IllegalArgumentException("Duplicate datasource id " + id);
        }
        dataNodes.put(id, dataSource);
    }


    @Override
    public DataSource lookup(String uid) {
        if (StringUtils.isNullOrEmpty(uid)) {
            throw new DataSourceException("DataSource id be not null.");
        }
        DataSource result = dataNodes.get(uid);
        return result;
    }

}
