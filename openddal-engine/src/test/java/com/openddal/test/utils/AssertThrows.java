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

package com.openddal.test.utils;

import com.openddal.message.DbException;

import java.lang.reflect.Method;
import java.sql.SQLException;

/**
 * Helper class to simplify negative testing. Usage:
 * <pre>
 * new AssertThrows() { public void test() {
 *     Integer.parseInt("not a number");
 * }};
 * </pre>
 */
public abstract class AssertThrows {

    /**
     * Create a new assertion object, and call the test method to verify the
     * expected exception is thrown.
     *
     * @param expectedExceptionClass the expected exception class
     */
    public AssertThrows(final Class<? extends Exception> expectedExceptionClass) {
        this(new ResultVerifier() {
            @Override
            public boolean verify(Object returnValue, Throwable t, Method m,
                                  Object... args) {
                if (t == null) {
                    throw new AssertionError("Expected an exception of type " +
                            expectedExceptionClass.getSimpleName() +
                            " to be thrown, but the method returned successfully");
                }
                if (!expectedExceptionClass.isAssignableFrom(t.getClass())) {
                    AssertionError ae = new AssertionError(
                            "Expected an exception of type\n" +
                                    expectedExceptionClass.getSimpleName() +
                                    " to be thrown, but the method under test " +
                                    "threw an exception of type\n" +
                                    t.getClass().getSimpleName() +
                                    " (see in the 'Caused by' for the exception " +
                                    "that was thrown)");
                    ae.initCause(t);
                    throw ae;
                }
                return false;
            }
        });
    }

    /**
     * Create a new assertion object, and call the test method to verify the
     * expected exception is thrown.
     */
    public AssertThrows() {
        this(new ResultVerifier() {
            @Override
            public boolean verify(Object returnValue, Throwable t, Method m,
                                  Object... args) {
                if (t != null) {
                    throw new AssertionError("Expected an exception " +
                            "to be thrown, but the method returned successfully");
                }
                // all exceptions are fine
                return false;
            }
        });
    }

    /**
     * Create a new assertion object, and call the test method to verify the
     * expected exception is thrown.
     *
     * @param expectedErrorCode the error code of the exception
     */
    public AssertThrows(final int expectedErrorCode) {
        this(new ResultVerifier() {
            @Override
            public boolean verify(Object returnValue, Throwable t, Method m,
                                  Object... args) {
                int errorCode;
                if (t instanceof DbException) {
                    errorCode = ((DbException) t).getErrorCode();
                } else if (t instanceof SQLException) {
                    errorCode = ((SQLException) t).getErrorCode();
                } else {
                    errorCode = 0;
                }
                if (errorCode != expectedErrorCode) {
                    AssertionError ae = new AssertionError(
                            "Expected an SQLException or DbException with error code " +
                                    expectedErrorCode);
                    ae.initCause(t);
                    throw ae;
                }
                return false;
            }
        });
    }

    private AssertThrows(ResultVerifier verifier) {
        try {
            test();
            verifier.verify(null, null, null);
        } catch (Exception e) {
            verifier.verify(null, e, null);
        }
    }

    /**
     * The test method that is called.
     *
     * @throws Exception the exception
     */
    public abstract void test() throws Exception;

}
