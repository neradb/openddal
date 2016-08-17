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
package com.openddal.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

import com.openddal.engine.Constants;
import com.openddal.engine.DbSettings;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.util.StringUtils;

/**
 * @author jorgie.li
 */
public class Driver implements java.sql.Driver {

    private static final Driver INSTANCE = new Driver();
    private static volatile boolean registered;

    static {
        load();
    }

    /**
     * INTERNAL
     */
    public static Driver load() {
        try {
            if (!registered) {
                synchronized (INSTANCE) {
                    registered = true;
                    DriverManager.registerDriver(INSTANCE);
                }
            }
        } catch (SQLException e) {
            DbException.traceThrowable(e);
        }
        return INSTANCE;
    }

    /**
     * INTERNAL
     */
    public static void unload() {
        try {
            if (registered) {
                synchronized (INSTANCE) {
                    registered = false;
                    DriverManager.deregisterDriver(INSTANCE);
                }
            }
        } catch (SQLException e) {
            DbException.traceThrowable(e);
        }
    }

    /**
     * INTERNAL
     */
    public static void setThreadContextClassLoader(Thread thread) {
        // Apache Tomcat: use the classloader of the driver to avoid the
        // following log message:
        // org.apache.catalina.loader.WebappClassLoader clearReferencesThreads
        // SEVERE: The web application appears to have started a thread named
        // ... but has failed to stop it.
        // This is very likely to create a memory leak.
        try {
            thread.setContextClassLoader(Driver.class.getClassLoader());
        } catch (Throwable t) {
            // ignore
        }
    }

    /**
     * Open a database connection. This method should not be called by an
     * application. Instead, the method DriverManager.getConnection should be
     * used.
     *
     * @param url  the database URL
     * @param info the connection properties
     * @return the new connection or null if the URL is not supported
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        try {
            if (!acceptsURL(url)) {
                return null;
            }
            if (info == null) {
                info = new Properties();
            }
            Properties prop = parseUrl(url, info);
            return new JdbcConnection(url, prop);
        } catch (Exception e) {
            throw DbException.toSQLException(e);
        }
    }

    /**
     * Check if the driver understands this URL. This method should not be
     * called by an application.
     *
     * @param url the database URL
     * @return if the driver understands the URL
     */
    @Override
    public boolean acceptsURL(String url) {
        if (url != null) {
            if (url.startsWith(Constants.START_URL)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get the major version number of the driver. This method should not be
     * called by an application.
     *
     * @return the major version number
     */
    @Override
    public int getMajorVersion() {
        return Constants.VERSION_MAJOR;
    }

    /**
     * Get the minor version number of the driver. This method should not be
     * called by an application.
     *
     * @return the minor version number
     */
    @Override
    public int getMinorVersion() {
        return Constants.VERSION_MINOR;
    }

    /**
     * Get the list of supported properties. This method should not be called by
     * an application.
     *
     * @param url  the database URL
     * @param info the connection properties
     * @return a zero length array
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        return new DriverPropertyInfo[0];
    }

    /**
     * Check if this driver is compliant to the JDBC specification. This method
     * should not be called by an application.
     *
     * @return true
     */
    @Override
    public boolean jdbcCompliant() {
        return true;
    }

    /**
     * [Not supported]
     */
    public Logger getParentLogger() {
        return null;
    }


    private Properties parseUrl(String url,Properties defaults) {
        Properties prop = new Properties();
        DbSettings defaultSettings = DbSettings.getDefaultSettings();
        for (Object k : defaults.keySet()) {
            String key = k.toString();
            if (prop.containsKey(key)) {
                throw DbException.get(ErrorCode.DUPLICATE_PROPERTY_1, key);
            }
            Object value = defaults.get(k);
            if (defaultSettings.containsKey(key)) {
                prop.put(key, value);
            }
        }
        
        int idx = url.indexOf(';');
        if (idx >= 0) {
            String settings = url.substring(idx + 1);
            url = url.substring(0, idx);
            String[] list = StringUtils.arraySplit(settings, ';', false);
            for (String setting : list) {
                if (setting.length() == 0) {
                    continue;
                }
                int equal = setting.indexOf('=');
                if (equal < 0) {
                    String format = Constants.URL_FORMAT;
                    throw DbException.get(ErrorCode.URL_FORMAT_ERROR_2, format, url);
                }
                String value = setting.substring(equal + 1);
                String key = setting.substring(0, equal);
                key = StringUtils.toUpperEnglish(key);
                if (!defaultSettings.containsKey(key)) {
                    throw DbException.get(ErrorCode.UNSUPPORTED_SETTING_1, key);
                }
                String old = prop.getProperty(key);
                if (old != null && !old.equals(value)) {
                    throw DbException.get(ErrorCode.DUPLICATE_PROPERTY_1, key);
                }
                prop.setProperty(key, value);
            }
        }
        return prop;
    }
    
}
