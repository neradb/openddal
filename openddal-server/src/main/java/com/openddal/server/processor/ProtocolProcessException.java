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

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class ProtocolProcessException extends Exception {

    private static final long serialVersionUID = 1L;

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
