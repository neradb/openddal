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

import java.io.InputStream;
import java.sql.SQLException;
import java.util.Properties;

import com.openddal.config.Configuration;
import com.openddal.config.parser.XmlConfigParser;
import com.openddal.dbobject.User;
import com.openddal.util.StringUtils;
import com.openddal.util.Utils;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class Engine implements SessionFactory {

    private static Engine INSTANCE = null;
    private static Database DATABASE = null;

    private Engine() {
        Runtime.getRuntime().addShutdownHook(new Shutdown());
    }

    public synchronized static Engine getInstance() {
        if (INSTANCE == null) {
            String configLocation = SysProperties.getEngineConfigLocation();
            INSTANCE = configuration(configLocation);
        }
        return INSTANCE;
    }

    private static Engine configuration(String configLocation) {
        if (StringUtils.isNullOrEmpty(configLocation)) {
            throw new IllegalArgumentException("Engine configLocation file must not be null");
        }
        InputStream source = null;
        try {
            source = Utils.getResourceAsStream(configLocation);
            if (source == null) {
                throw new IllegalArgumentException(
                        "Can't load engine configLocation " + configLocation + " from classpath.");
            }
            XmlConfigParser parser = new XmlConfigParser(source);
            Configuration configuration = parser.parse();
            DATABASE = new Database(configuration);
            return new Engine();
        } finally {
            try {
                source.close();
            } catch (Exception e) {
                //ignored
            }
        }
    }

    @Override
    public SessionInterface createSession(String url, Properties ci) throws SQLException {
        if (StringUtils.isNullOrEmpty(url)) {
            throw new IllegalArgumentException();
        }
        return INSTANCE.openSession(url, ci);
    }

    private Session openSession(String url, Properties info) {
        String userName = info.getProperty("user");
        String password = info.getProperty("password");
        if (userName == null) {
            userName = Database.SYSTEM_USER_NAME;
        }
        User user = DATABASE.findUser(userName);
        if (user == null) {
            // users is the last thing we add, so if no user is around,
            // the database is new (or not initialized correctly)
            user = new User(DATABASE, DATABASE.allocateObjectId(), userName);
            user.setAdmin(true);
            user.setPassword(password);
            DATABASE.addDatabaseObject(user);
        }
        User userObject = DATABASE.getUser(userName);
        Session session = DATABASE.createSession(userObject);
        return session;

    }


    private static class Shutdown extends Thread {
        private Shutdown() {
            super("database-engine-closer");
        }
        @Override
        public void run() {
            synchronized (Engine.class) {
                try {
                    DATABASE.close();
                } catch (Exception e) {

                }
            }
        }
    }

}
