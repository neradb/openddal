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
// Created on 2015年3月27日
// $Id$

package com.openddal.route.rule;

import java.io.Serializable;

import com.openddal.util.StringUtils;

/**
 * @author jorgie.li
 */
public class ObjectNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private String shardName;

    private String catalog;

    private String schema;

    private String objectName;

    private String suffix;


    public ObjectNode(String shardName, String objectName) {
        this(shardName, objectName, null);
    }

    public ObjectNode(String shardName, String objectName, String suffix) {
        this(shardName, null, null, objectName, suffix);
    }

    public ObjectNode(String shardName, String catalog, String schema, String objectName, String suffix) {
        this.shardName = shardName;
        this.catalog = catalog;
        this.schema = schema;
        this.objectName = objectName;
        this.suffix = suffix;
    }

    /**
     * @return the shardName
     */
    public String getShardName() {
        return shardName;
    }

    /**
     * @return the tableName
     */
    public String getObjectName() {
        return objectName;
    }

    /**
     * @return the suffix
     */
    public String getSuffix() {
        return suffix;
    }

    /**
     * @return the catalog
     */
    public String getCatalog() {
        return catalog;
    }

    /**
     * @return the schema
     */
    public String getSchema() {
        return schema;
    }

    /**
     * @param catalog the catalog to set
     */
    public void setCatalog(String catalog) {
        this.catalog = catalog;
    }

    /**
     * @param shardName the shardName to set
     */
    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    /**
     * @param schema the schema to set
     */
    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * @param objectName the objectName to set
     */
    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    /**
     * @param suffix the suffix to set
     */
    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public String getCompositeObjectName() {
        StringBuilder fullName = new StringBuilder();
        if (!StringUtils.isNullOrEmpty(catalog)) {
            fullName.append(catalog).append(".");
        }
        if (!StringUtils.isNullOrEmpty(schema)) {
            fullName.append(schema).append(".");
        }
        fullName.append(objectName);
        if (!StringUtils.isNullOrEmpty(suffix)) {
            fullName.append(suffix);
        }
        return fullName.toString();
    }

    public String getQualifiedObjectName() {
        StringBuilder fullName = new StringBuilder();
        fullName.append(objectName);
        if (!StringUtils.isNullOrEmpty(suffix)) {
            fullName.append(suffix);
        }
        return fullName.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((catalog == null) ? 0 : catalog.hashCode());
        result = prime * result + ((objectName == null) ? 0 : objectName.hashCode());
        result = prime * result + ((schema == null) ? 0 : schema.hashCode());
        result = prime * result + ((shardName == null) ? 0 : shardName.hashCode());
        result = prime * result + ((suffix == null) ? 0 : suffix.hashCode());
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
        ObjectNode other = (ObjectNode) obj;
        if (catalog == null) {
            if (other.catalog != null)
                return false;
        } else if (!catalog.equals(other.catalog))
            return false;
        if (objectName == null) {
            if (other.objectName != null)
                return false;
        } else if (!objectName.equals(other.objectName))
            return false;
        if (schema == null) {
            if (other.schema != null)
                return false;
        } else if (!schema.equals(other.schema))
            return false;
        if (shardName == null) {
            if (other.shardName != null)
                return false;
        } else if (!shardName.equals(other.shardName))
            return false;
        if (suffix == null) {
            if (other.suffix != null)
                return false;
        } else if (!suffix.equals(other.suffix))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ObjectNode [shardName=" + shardName + ", objectName=" + getCompositeObjectName() + "]";
    }

    

}
