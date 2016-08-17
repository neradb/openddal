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
// Created on 2015年4月13日
// $Id$

package com.openddal.repo;

/**
 * @author jorgie.li
 */
public class Options {

    public static final int DEFAULT_TRYTIMES = 3;

    public int retry = DEFAULT_TRYTIMES;
    public String dbid;
    public String shardName;
    public boolean readOnly;


    public static Options build() {
        Options optional = new Options();
        return optional;
    }


    /**
     * @param dbid the dbid to set
     */
    public Options dbid(String dbid) {
        this.dbid = dbid;
        return this;
    }


    /**
     * @param shardName the shardName to set
     */
    public Options shardName(String shardName) {
        this.shardName = shardName;
        return this;
    }


    /**
     * @param readOnly the readOnly to set
     */
    public Options readOnly(boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    /**
     * @param retry the retry to set
     */
    public Options retry(int retry) {
        this.retry = retry;
        return this;
    }


}
