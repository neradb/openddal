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
package com.openddal.server.core;

import java.sql.SQLException;

import com.openddal.message.DbException;
import com.openddal.server.util.ErrorCode;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class ProcessException extends Exception {

    private static final long serialVersionUID = 1L;

    public static ProcessException convert(Throwable e) {
        if (e instanceof ProcessException) {
            return (ProcessException) e;
        } else if (e instanceof DbException) {
            DbException dbe = (DbException) e;
            return new ProcessException(dbe.getErrorCode(), dbe.getMessage(), e);
        } else if (e instanceof SQLException) {
            SQLException sqle = (SQLException) e;
            return new ProcessException(sqle.getErrorCode(), sqle.getMessage(), e);
        } else if (e instanceof OutOfMemoryError) {
            return new ProcessException(ErrorCode.ER_OUTOFMEMORY, "ER_OUTOFMEMORY", e);
        } else {
            return new ProcessException(ErrorCode.ER_UNKNOWN_ERROR, "ERR_GENERAL_EXCEPION", e);
        }
    }

    public static ProcessException get(int errorCode, String message) {
        return new ProcessException(errorCode, message);
    }

    protected int errorCode;

    public ProcessException(int errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }
    
    public ProcessException(int errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }


    public ProcessException(int errorCode, Throwable cause) {
        super(cause);
        this.errorCode = errorCode;
    }


    public int getErrorCode() {
        return errorCode;
    }
}
