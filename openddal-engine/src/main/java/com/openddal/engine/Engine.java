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
package com.openddal.engine;

import java.sql.SQLException;
import java.util.Properties;

import com.openddal.dbobject.User;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.util.StringUtils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class Engine implements SessionFactory {

    public static final String ENGINE_CONFIG_PROPERTY_NAME = "ENGINE_CONFIG_LOCATION";
    private static Engine implicitInstance;
    private static String implicitEngineConfig;

    private Database database = null;

    public Engine(Database database) {
        this.database = database;
        Runtime.getRuntime().addShutdownHook(new Shutdown());
    }

    public synchronized static Engine getImplicitEngine(Properties setting) {  
        String configLocation = (String) setting.remove(ENGINE_CONFIG_PROPERTY_NAME);
        if (StringUtils.isNullOrEmpty(configLocation)) {
            throw DbException.get(ErrorCode.INVALID_ENGINE_CONFIG_1, configLocation);
        }
        if (implicitInstance == null) {
            implicitEngineConfig = configLocation;
            implicitInstance = (Engine) SessionFactoryBuilder.newBuilder().fromXml(implicitEngineConfig)
                    .applySettings(setting).build();
        } else {
            if (!StringUtils.equals(implicitEngineConfig, configLocation)) {
                String msg = configLocation + ", The implicit engine was established with " + implicitEngineConfig;
                throw DbException.get(ErrorCode.INVALID_ENGINE_CONFIG_1, msg);
            }
        }
        return implicitInstance;
    }

    public synchronized static boolean implicitEngineExists() {
        return implicitInstance != null;
    }

    @Override
    public SessionInterface createSession(Properties ci) throws SQLException {
        String userName = ci.getProperty("user");
        String password = ci.getProperty("password");
        if (userName == null) {
            userName = Database.SYSTEM_USER_NAME;
        }
        User user = database.findUser(userName);
        if (user == null) {
            // users is the last thing we add, so if no user is around,
            // the database is new (or not initialized correctly)
            user = new User(database, userName);
            user.setAdmin(true);
            user.setPassword(password);
            database.addDatabaseObject(user);
        }
        User userObject = database.getUser(userName);
        Session session = database.createSession(userObject);
        return session;

    }

    private class Shutdown extends Thread {
        private Shutdown() {
            super("database-closer");
        }

        @Override
        public void run() {
            if (database != null) {
                database.close();
            }
        }
    }

}
