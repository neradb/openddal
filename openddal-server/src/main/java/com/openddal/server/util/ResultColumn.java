package com.openddal.server.util;

import com.openddal.result.ResultInterface;
import com.openddal.server.mysql.proto.ColumnDefinition;
import com.openddal.value.DataType;

/**
 * A result set column of a remote result.
 */
public class ResultColumn {

    public static ColumnDefinition getColumn(ResultInterface result, int i) {
        ColumnDefinition columnPacket = new ColumnDefinition();
        columnPacket.catalog = "def";
        columnPacket.org_name = result.getColumnName(i);
        columnPacket.name = result.getAlias(i);
        columnPacket.org_table = result.getTableName(i);
        columnPacket.table = result.getTableName(i);
        columnPacket.schema = result.getSchemaName(i);
        columnPacket.flags = toFlag(result, i);
        columnPacket.columnLength = result.getDisplaySize(i);
        columnPacket.decimals = result.getColumnScale(i);

        int sqlType = DataType.convertTypeToSQLType(result.getColumnType(i));
        int javaType = MysqlDefs.javaTypeDetect(sqlType, (int) columnPacket.decimals);
        columnPacket.type = (byte) (MysqlDefs.javaTypeMysql(javaType) & 0xff);
        return columnPacket;

    }

    public static int toFlag(ResultInterface result, int idx) {
        int flags = 0;
        if (result.getNullable(idx) == 1) {
            flags |= 0001;
        }
        flags |= 0020;
        if (result.isAutoIncrement(idx)) {
            flags |= 0200;
        }
        return flags;
    }

}
