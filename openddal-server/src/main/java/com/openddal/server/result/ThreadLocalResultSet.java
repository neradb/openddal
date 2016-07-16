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

package com.openddal.server.result;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A thread-local-based implementation which instance can be reusable. 
 * 
 * @author little-pan
 * @since 2016-07-17
 */
public class ThreadLocalResultSet extends AbstractResultSet {
	
	protected final static ThreadLocal<ResultSetIterator> ITERATOR = 
		new ThreadLocal<ResultSetIterator>(){
		
		@Override
		protected ResultSetIterator initialValue(){
			return (new SimpleResultSetIterator());
		}
	};
	
	@Override
	protected ResultSetIterator iterator(){
		final ResultSetIterator iterator = ITERATOR.get();
		iterator.setResultSet(this);
		return iterator;
	}

	@Override
	public void close() {
		ITERATOR.remove();
	}
	
	@Override
	public ResultSet reset()throws SQLException{
		return super.reset();
	}

}
