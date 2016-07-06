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
package com.openddal.message;

import java.util.concurrent.ConcurrentMap;

import com.openddal.util.New;

/**
 * The trace mechanism is the logging facility of this database. There is
 * usually one trace system per database. It is called 'trace' because the term
 * 'log' is already used in the database domain and means 'transaction log'. It
 * is possible to write after close was called, but that means for each write
 * the file will be opened and closed again (which is slower).
 */
public class TraceSystem {

    /**
     * The parent trace level should be used.
     */
    public static final int PARENT = -1;

    /**
     * This trace level means nothing should be written.
     */
    public static final int OFF = 0;

    /**
     * This trace level means only errors should be written.
     */
    public static final int ERROR = 1;

    /**
     * This trace level means errors and informational messages should be
     * written.
     */
    public static final int INFO = 2;

    /**
     * This trace level means all type of messages should be written.
     */
    public static final int DEBUG = 3;

    private final ConcurrentMap<String, Trace> traces = New.concurrentHashMap();
    
    /**
     * Get or create a trace object for this module id. Trace modules with id
     * are cached.
     *
     * @param moduleId module id
     * @return the trace object
     */
    public Trace getTrace(String module) {
        Trace t = traces.get(module);
        if (t == null) {
            Trace newTrace = new Trace(new TraceWriterAdapter(module));
            t = traces.putIfAbsent(module, newTrace);
            if (t == null) {
                t = newTrace;
            }
        }
        return t;
    }
}
