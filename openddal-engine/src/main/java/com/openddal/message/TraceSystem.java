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

import java.util.HashMap;

import com.openddal.util.New;

/**
 * The trace mechanism is the logging facility of this database. There is
 * usually one trace system per database. It is called 'trace' because the term
 * 'log' is already used in the database domain and means 'transaction log'. It
 * is possible to write after close was called, but that means for each write
 * the file will be opened and closed again (which is slower).
 */
public class TraceSystem implements TraceWriter {

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


    private HashMap<String, Trace> traces;
    private TraceWriter writer = new TraceWriterAdapter();

    /**
     * Get or create a trace object for this module. Trace modules with names
     * such as "JDBC[1]" are not cached (modules where the name ends with "]").
     * All others are cached.
     *
     * @param module the module name
     * @return the trace object
     */
    public synchronized Trace getTrace(String module) {
        if (module.endsWith("]")) {
            return new Trace(writer, module);
        }
        if (traces == null) {
            traces = New.hashMap(16);
        }
        Trace t = traces.get(module);
        if (t == null) {
            t = new Trace(writer, module);
            traces.put(module, t);
        }
        return t;
    }
    
    
    /**
     * Set the file trace level.
     *
     * @param level the new level
     */
    public void setAdapterClass(String adapterClass) {
        try {
            writer = (TraceWriter) Class.forName(adapterClass).newInstance();
        } catch (Throwable e) {
            e = DbException.get(ErrorCode.CLASS_NOT_FOUND_1, e, adapterClass);
            write(ERROR, Trace.DATABASE, adapterClass, e);
            return;
        }
    
    }

    @Override
    public boolean isEnabled(int level) {
        return writer.isEnabled(level);
    }

    @Override
    public void write(int level, String module, String s, Throwable t) {
        writer.write(level, module, s, t);
    }


    @Override
    public void setName(String name) {
        writer.setName(name);
    }

}
