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
package com.openddal.server;

import java.io.IOException;
import java.sql.SQLException;

import com.openddal.message.DbException;
import com.openddal.server.util.ErrorCode;

/**
 * @author jorgie.li
 *
 */
public class ServerException extends RuntimeException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    private ServerException(SQLException e) {
        super(e.getMessage(), e);
    }

    public static ServerException get(int errorCode, String message, Throwable cause) {
        return new ServerException(getQueryException(errorCode, "HY0000", message, cause));
    }

    public static ServerException get(int errorCode, String message) {
        return new ServerException(getQueryException(errorCode, "HY0000", message, null));
    }
    
    public static ServerException get(String message) {
        return new ServerException(getQueryException(-1, "HY0000", message, null));
    }

    public static SQLException toSQLException(Throwable e) {
        if (e instanceof SQLException) {
            return (SQLException) e;
        }
        return convert(e).getSQLException();
    }

    private static SQLException getQueryException(int errorCode, String sqlstate, String message, Throwable cause) {
        return new SQLException(message, sqlstate, errorCode, cause);
    }

    public static ServerException convert(Throwable e) {
        if (e instanceof ServerException) {
            return (ServerException) e;
        } else if (e instanceof SQLException) {
            return new ServerException((SQLException) e);
        } else if (e instanceof DbException) {
            SQLException sqle = ((DbException) e).getSQLException();
            return new ServerException(sqle);
        } else if (e instanceof IOException) {
            return get(ErrorCode.ER_UNKNOWN_ERROR, e.toString(), e);
        } else if (e instanceof OutOfMemoryError) {
            return get(ErrorCode.ER_OUTOFMEMORY, e.toString(), e);
        } else if (e instanceof StackOverflowError || e instanceof LinkageError) {
            return get(ErrorCode.ER_UNKNOWN_ERROR, e.toString(), e);
        } else if (e instanceof Error) {
            throw (Error) e;
        }
        return get(ErrorCode.ER_UNKNOWN_ERROR, e.toString(), e);
    }

    public SQLException getSQLException() {
        return (SQLException) getCause();
    }

}
