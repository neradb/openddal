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
package com.openddal.server.mysql.packet;

import java.util.List;

/**
 * mysql sql result status. for example ,query header result query rowset resut
 * ,ok result,
 * 
 * @author wuzhih
 * 
 */
public class ResultStatus {
	public static final int RESULT_STATUS_INIT = 0;
	public static final int RESULT_STATUS_HEADER = 1;
	public static final int RESULT_STATUS_FIELD_EOF = 2;

	private int resultStatus;
	private byte[] header;
	private List<byte[]> fields;

	public int getResultStatus() {
		return resultStatus;
	}

	public void setResultStatus(int resultStatus) {
		this.resultStatus = resultStatus;
	}

	public byte[] getHeader() {
		return header;
	}

	public void setHeader(byte[] header) {
		this.header = header;
	}

	public List<byte[]> getFields() {
		return fields;
	}

	public void setFields(List<byte[]> fields) {
		this.fields = fields;
	}

}
