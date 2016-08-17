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

import java.util.HashMap;
import java.util.Properties;

import com.openddal.dbobject.User;
import com.openddal.util.New;

/**
 * @author jorgie.li
 */
public class Engine implements SessionFactory {
    
    private static final HashMap<String, SessionFactory> EMBEDDED_ENGINES = New.hashMap();

    private Database database = null;

    Engine(Database database) {
        this.database = database;
        Runtime.getRuntime().addShutdownHook(new Shutdown());
    }

    public synchronized static SessionFactory connectEmbedded(String url, Properties prop) {  
        SessionFactory sessionFactory = EMBEDDED_ENGINES.get(url);
        if (sessionFactory == null) {
            String configLocation = getConfigLocation(url);
            sessionFactory = SessionFactoryBuilder.newBuilder().fromXml(configLocation).applySettings(prop).build();
            EMBEDDED_ENGINES.put(url, sessionFactory);
        }
        return sessionFactory;
    }
    
    private static String getConfigLocation(String url) {
        int idx = url.indexOf(';');
        String configLocation;
        if (idx >= 0) {
            configLocation = url.substring(Constants.START_URL.length(), idx);
        } else {
            configLocation = url.substring(Constants.START_URL.length());
        }
        return configLocation;
    }

    @Override
    public Session createSession(Properties ci) {
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
