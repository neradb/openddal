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
package com.openddal.server;

import com.openddal.util.StringUtils;

public class ServerArgs {

	public int port = -1;
	public boolean ssl;
	
	public String protocol;

	public int bossThreads = 2;
	public int workerThreads = 0;
	public int maxThreads = 1000;
	public int keepAliveTime = 10000;

	public int socketTimeoutMills = -1;
	public int shutdownTimeoutMills = 10000;

	public int sendBuff = -1;
	public int recvBuff = -1;

	public String configFile;


	public ServerArgs port(int port) {
		this.port = port;
		return this;
	}
	
	public ServerArgs ssl(boolean ssl) {
        this.ssl = ssl;
        return this;
    }
	
	public ServerArgs protocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

	public ServerArgs bossThreads(int bossThreads) {
		this.bossThreads = bossThreads;
		return this;
	}

	public ServerArgs workerThreads(int workerThreads) {
		this.workerThreads = workerThreads;
		return this;
	}

	public ServerArgs maxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
		return this;
	}
	
	public ServerArgs keepAliveTime(int keepAliveTime) {
        this.keepAliveTime = keepAliveTime;
        return this;
    }

	public ServerArgs socketTimeoutMills(int socketTimeoutMills) {
		this.socketTimeoutMills = socketTimeoutMills;
		return this;
	}

	public ServerArgs shutdownTimeoutMills(int shutdownTimeoutMills) {
		this.shutdownTimeoutMills = shutdownTimeoutMills;
		return this;
	}

	public ServerArgs sendBuff(int sendBuff) {
		this.sendBuff = sendBuff;
		return this;
	}

	public ServerArgs recvBuff(int recvBuff) {
		this.recvBuff = recvBuff;
		return this;
	}

	public ServerArgs configFile(String configFile) {
		this.configFile = configFile;
		return this;
	}
	
	
	public void validate() {
        if (port < 0) {
            throw ServerException.get("port " + port + " is wrong.");
        }
        if (StringUtils.isNullOrEmpty(configFile)) {
            throw ServerException.get("Do not specify a engine config file. Please use -configFile option to specify engine config file.");
        }
    }

}
