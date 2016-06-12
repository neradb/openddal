/*
 * Copyright 2014-2015 the original author or authors
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
// Created on 2015年4月12日
// $Id$

package com.openddal.excutor.effects;

import com.openddal.command.ddl.CreateIndex;
import com.openddal.excutor.ExecutionFramework;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public class CreateIndexExecutor extends ExecutionFramework<CreateIndex>{

    /**
     * @param session
     * @param prepared
     */
    public CreateIndexExecutor(CreateIndex prepared) {
        super(prepared);
    }

    @Override
    protected void doPrepare() {
        // TODO Auto-generated method stub
        
    }

    @Override
    protected String doExplain() {
        // TODO Auto-generated method stub
        return null;
    }


}
