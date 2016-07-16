package com.openddal.server.result;

import java.sql.SQLException;

public interface ResultSetIterator {
	
	boolean nextFromSource() throws SQLException;
	
	boolean nextFromRows();
	
	int getRow();
	
	Object get(int column) throws SQLException;
	
	boolean wasNull();
	
	void update(int column, Object value);
	
	void reset();
	
	void setResultSet(AbstractResultSet resultSet);
	
}
