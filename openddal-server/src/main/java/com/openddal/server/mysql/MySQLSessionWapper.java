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
package com.openddal.server.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.openddal.server.core.Session;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLSessionWapper implements Session {

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#setAttachment(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public <T> T setAttachment(String key, T value) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#getAttachment(java.lang.String)
     */
    @Override
    public <T> T getAttachment(String key) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#getThreadId()
     */
    @Override
    public long getThreadId() {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#getUser()
     */
    @Override
    public String getUser() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#getSchema()
     */
    @Override
    public String getSchema() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#getCharset()
     */
    @Override
    public String getCharset() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#getCharsetIndex()
     */
    @Override
    public int getCharsetIndex() {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#setCharset(java.lang.String)
     */
    @Override
    public boolean setCharset(String charset) {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#setCharsetIndex(int)
     */
    @Override
    public boolean setCharsetIndex(int parseInt) {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#executeQuery(java.lang.String)
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#executeUpdate(java.lang.String)
     */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        // TODO Auto-generated method stub
        return 0;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.openddal.server.core.Session#close()
     */
    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

}
