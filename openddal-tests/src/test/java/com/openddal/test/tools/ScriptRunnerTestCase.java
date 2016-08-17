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
// Created on 2015年4月7日
// $Id$

package com.openddal.test.tools;

import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;

import com.openddal.test.BaseTestCase;
import com.openddal.util.ScriptRunner;
import com.openddal.util.Utils;

import junit.framework.Assert;

/**
 * @author jorgie.li
 */
public class ScriptRunnerTestCase extends BaseTestCase {


    public void runCreateScript() throws Exception {
        Connection conn = getConnection();
        runOn(conn);
    }

    private void runOn(Connection conn) {
        ScriptRunner runner = new ScriptRunner(conn);
        runner.setAutoCommit(false);
        runner.setStopOnError(true);

        String resource = "script/mysql_script.sql";
        Reader reader = new InputStreamReader(Utils.getResourceAsStream(resource));

        try {
            runner.runScript(reader);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    public void runOnH2() throws Exception {
        Connection conn = getH2Connection();
        runOn(conn);
    }
    
    public static void main(String[] args) throws Exception {
        ScriptRunnerTestCase case1 = new ScriptRunnerTestCase();
        case1.runCreateScript();
    }
}
