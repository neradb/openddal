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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceWriterAdapter implements TraceWriter {

    private final Logger logger;

    public TraceWriterAdapter(String module) {
        this.logger = LoggerFactory.getLogger("ddal-engine-" + module);
    }

    @Override
    public boolean isEnabled(int level) {
        switch (level) {
            case TraceSystem.DEBUG:
                return logger.isDebugEnabled();
            case TraceSystem.INFO:
                return logger.isInfoEnabled();
            case TraceSystem.ERROR:
                return logger.isErrorEnabled();
            default:
                return false;
        }
    }

    @Override
    public void write(int level, String s, Throwable t) {
        if (isEnabled(level)) {
            switch (level) {
                case TraceSystem.DEBUG:
                    logger.debug(s, t);
                    break;
                case TraceSystem.INFO:
                    logger.info(s, t);
                    break;
                case TraceSystem.ERROR:
                    logger.error(s, t);
                    break;
                default:
            }
        }
    }

}
