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
// Created on 2015年4月10日
// $Id$

package com.openddal.executor;

import com.openddal.engine.Session;
import com.openddal.executor.cursor.Cursor;
import com.openddal.message.DbException;

/**
 * @author jorgie.li
 */
public interface Executor {
    /**
     * Execute the query.
     *
     * @return the result cursor
     * @throws DbException if it is not a query
     */
    Cursor query(Session s);

    /**
     * Execute the statement.
     *
     * @return the update count
     * @throws DbException if it is a query
     */
    int update(Session s);

    /**
     * Get the PreparedExecutor with the execution explain.
     *
     * @return the execution explain
     */
    String explain(Session s);

}
