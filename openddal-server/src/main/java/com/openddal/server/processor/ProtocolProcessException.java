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
package com.openddal.server.processor;

import java.io.IOException;
import java.sql.SQLException;

import com.openddal.jdbc.JdbcSQLException;
import com.openddal.message.DbException;
import com.openddal.server.mysql.ErrorCode;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class ProtocolProcessException extends SQLException {

    private static final long serialVersionUID = 1L;
    
    
    public static ProtocolProcessException convert(Throwable e) {
        if (e instanceof ProtocolProcessException) {
            return (ProtocolProcessException) e;
        } else if (e instanceof DbException) {
             DbException dbe = (DbException) e;
            return new ProtocolProcessException(dbe.getErrorCode(),e.toString(),e);
        } else if (e instanceof IOException) {
            return get(ErrorCode.ER_NET_READ_ERROR, e.toString(),e);
        } else if (e instanceof JdbcSQLException) {
            JdbcSQLException sqle = (JdbcSQLException) e;
            return new ProtocolProcessException(sqle.getErrorCode(),e.toString(),e);
        } else if (e instanceof OutOfMemoryError) {
            return new ProtocolProcessException(ErrorCode.ER_OUTOFMEMORY,e.toString(),e);
        } else {
            return new ProtocolProcessException(ErrorCode.ER_UNKNOWN_ERROR,e.toString(),e);
        }
    }
    
    public static ProtocolProcessException get(int errorCode, String message) {
        return new ProtocolProcessException(errorCode, message);
    }
    
    public static ProtocolProcessException get(int errorCode, String message, Throwable cause) {
        return new ProtocolProcessException(errorCode, message, cause);
    }

    protected int errorCode;
    protected String errorMessage;
    

    public ProtocolProcessException(int errorCode, String message) {
        super(message);
    }

    public ProtocolProcessException(int errorCode, String message, Throwable cause) {
        super(cause);
    }

    @Override
    public String getMessage() {
        return errorCode + ":" + errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String retMessage) {
        this.errorMessage = retMessage;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
