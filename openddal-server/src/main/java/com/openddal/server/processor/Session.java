/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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
package com.openddal.server.processor;

import java.sql.Connection;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public interface Session {

    <T> T setAttachment(String key, T value);

    <T> T getAttachment(String key);

    long getSessionId();

    String getUser();

    void setUser(String user);

    String getSchema();

    String getCharset();

    State getState();

    void setState(State state);

    Connection getEngineConnection();

    enum State {
        NEW, CONNECTIONING, CONNECTIONED
    }

}
