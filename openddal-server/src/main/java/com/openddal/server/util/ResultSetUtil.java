package com.openddal.server.util;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * 
 * @author struct
 * 
 */
public class ResultSetUtil {

	public static int toFlag(ResultSetMetaData metaData, int column)
			throws SQLException {

		int flags = 0;
		if (metaData.isNullable(column) == 1) {
			flags |= 0001;
		}

		if (metaData.isSigned(column)) {
			flags |= 0020;
		}

		if (metaData.isAutoIncrement(column)) {
			flags |= 0200;
		}
		return flags;
	}

}
