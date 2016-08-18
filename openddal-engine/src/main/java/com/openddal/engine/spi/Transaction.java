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
package com.openddal.engine.spi;

/**
 * Wraps a database connection. Handles the connection lifecycle that comprises:
 * its creation, preparation, commit/rollback and close.
 */
public interface Transaction {
    
    void setIsolation(int level);

    void setReadOnly(boolean readOnly);

    void commit();

    void rollback();

    void addSavepoint(String name);

    void rollbackToSavepoint(String name);
    
    void releaseSavepoint(String name);
    
    boolean isClosed();

    void close();

    Long getId();

}
