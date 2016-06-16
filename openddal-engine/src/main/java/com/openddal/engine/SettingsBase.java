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

import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.util.Utils;

/**
 * The base class for settings.
 */
public class SettingsBase {

    private final HashMap<String, String> settings;

    protected SettingsBase(HashMap<String, String> s) {
        this.settings = s;
    }

    /**
     * Get the setting for the given key.
     *
     * @param key          the key
     * @param defaultValue the default value
     * @return the setting
     */
    protected boolean get(String key, boolean defaultValue) {
        String s = get(key, "" + defaultValue);
        try {
            return Boolean.parseBoolean(s);
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1,
                    e, "key:" + key + " value:" + s);
        }
    }

    /**
     * Get the setting for the given key.
     *
     * @param key          the key
     * @param defaultValue the default value
     * @return the setting
     */
    protected int get(String key, int defaultValue) {
        String s = get(key, "" + defaultValue);
        try {
            return Integer.decode(s);
        } catch (NumberFormatException e) {
            throw DbException.get(ErrorCode.DATA_CONVERSION_ERROR_1,
                    e, "key:" + key + " value:" + s);
        }
    }

    /**
     * Get the setting for the given key.
     *
     * @param key          the key
     * @param defaultValue the default value
     * @return the setting
     */
    protected String get(String key, String defaultValue) {
        StringBuilder buff = new StringBuilder();
        boolean nextUpper = false;
        for (char c : key.toCharArray()) {
            if (c == '_') {
                nextUpper = true;
            } else {
                // Character.toUpperCase / toLowerCase ignores the locale
                buff.append(nextUpper ? Character.toUpperCase(c) : Character.toLowerCase(c));
                nextUpper = false;
            }
        }
        String property = buff.toString();
        String sysProperty = "ddal." + buff.toString();
        String v = settings.get(property);
        if (v == null) {
            v = Utils.getProperty(sysProperty, defaultValue);
            settings.put(property, v);
        }
        return v;
    }

    /**
     * Check if the settings contains the given key.
     *
     * @param k the key
     * @return true if they do
     */
    public boolean containsKey(String k) {
        return settings.containsKey(k);
    }

    /**
     * Get all settings.
     *
     * @return the settings
     */
    public HashMap<String, String> getSettings() {
        return settings;
    }

}
