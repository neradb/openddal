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

import com.openddal.util.MathUtils;
import com.openddal.util.Utils;

public class SysProperties {

    public static final String FILE_ENCODING = Utils.getProperty("file.encoding", "Cp1252");

    public static final String FILE_SEPARATOR = Utils.getProperty("file.separator", "/");

    public static final String JAVA_SPECIFICATION_VERSION = Utils.getProperty("java.specification.version", "1.4");

    public static final String LINE_SEPARATOR = Utils.getProperty("line.separator", "\n");

    public static final String USER_HOME = Utils.getProperty("user.home", "");

    public static final String ALLOWED_CLASSES = Utils.getProperty("ddal.allowedClasses", "*");

    public static final boolean CHECK = Utils.getProperty("ddal.check", true);

    public static final boolean CHECK2 = Utils.getProperty("ddal.check2", false);

    public static final String CLIENT_TRACE_DIRECTORY = Utils.getProperty("ddal.clientTraceDirectory", "trace.db/");

    public static final int COLLATOR_CACHE_SIZE = Utils.getProperty("ddal.collatorCacheSize", 32000);

    public static final boolean JAVA_SYSTEM_COMPILER = Utils.getProperty("ddal.javaSystemCompiler", true);

    public static final int LOB_FILES_PER_DIRECTORY = Utils.getProperty("ddal.lobFilesPerDirectory", 256);

    public static final int LOB_CLIENT_MAX_SIZE_MEMORY = Utils.getProperty("ddal.lobClientMaxSizeMemory", 1024 * 1024);

    public static final int MAX_FILE_RETRY = Math.max(1, Utils.getProperty("ddal.maxFileRetry", 16));

    public static final int MAX_MEMORY_ROWS = getAutoScaledForMemoryProperty("ddal.maxMemoryRows", 10000);

    public static final long MAX_TRACE_DATA_LENGTH = Utils.getProperty("ddal.maxTraceDataLength", 65535);

    public static final boolean OBJECT_CACHE = Utils.getProperty("ddal.objectCache", true);

    public static final int OBJECT_CACHE_MAX_PER_ELEMENT_SIZE = Utils.getProperty("ddal.objectCacheMaxPerElementSize",
            4096);
    
    public static final int OBJECT_CACHE_SIZE = MathUtils.nextPowerOf2(Utils.getProperty("ddal.objectCacheSize", 1024));

    public static final boolean OLD_STYLE_OUTER_JOIN = Utils.getProperty("ddal.oldStyleOuterJoin", false);

    public static final String PG_DEFAULT_CLIENT_ENCODING = Utils.getProperty("ddal.pgClientEncoding", "UTF-8");

    public static final String PREFIX_TEMP_FILE = Utils.getProperty("ddal.prefixTempFile", "ddal.temp");

    public static final int SERVER_RESULT_SET_FETCH_SIZE = Utils.getProperty("ddal.serverResultSetFetchSize", 100);

    public static final int SOCKET_CONNECT_RETRY = Utils.getProperty("ddal.socketConnectRetry", 16);

    public static final int SOCKET_CONNECT_TIMEOUT = Utils.getProperty("ddal.socketConnectTimeout", 2000);

    public static final boolean SORT_BINARY_UNSIGNED = Utils.getProperty("ddal.sortBinaryUnsigned", true);

    public static final boolean SORT_NULLS_HIGH = Utils.getProperty("ddal.sortNullsHigh", false);

    public static final String SYNC_METHOD = Utils.getProperty("ddal.syncMethod", "sync");

    public static final boolean TRACE_IO = Utils.getProperty("ddal.traceIO", false);

    public static final boolean USE_THREAD_CONTEXT_CLASS_LOADER = Utils.getProperty("ddal.useThreadContextClassLoader",
            false);

    public static final String JAVA_OBJECT_SERIALIZER = Utils.getProperty("ddal.javaObjectSerializer", null);
    
    public static final int THREAD_QUEUE_SIZE = Integer.getInteger("ddal.threadqueue.size", 20480);
    
    public static final int THREAD_POOL_SIZE_CORE = Utils.getProperty("ddal.threadpool.size.core", Runtime.getRuntime().availableProcessors() * 2);
    
    public static final int THREAD_POOL_SIZE_MAX = Utils.getProperty("ddal.threadpool.size.max", Runtime.getRuntime().availableProcessors() * 20);
    
    public static boolean serializeJavaObject = Utils.getProperty("ddal.serializeJavaObject", true);

    public static final String SERVERUSER_CONFIG_LOCATION = Utils.getProperty("ddal.serverUserConfigLocation", "users.properties");

    private SysProperties() {
        // utility class
    }


    /**
     * This method attempts to auto-scale some of our properties to take
     * advantage of more powerful machines out of the box. We assume that our
     * default properties are set correctly for approx. 1G of memory, and scale
     * them up if we have more.
     */
    private static int getAutoScaledForMemoryProperty(String key, int defaultValue) {
        String s = Utils.getProperty(key, null);
        if (s != null) {
            try {
                return Integer.decode(s).intValue();
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return Utils.scaleForAvailableMemory(defaultValue);
    }

}
