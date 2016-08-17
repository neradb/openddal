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
// Created on 2014年12月25日
// $Id$
package com.openddal.dbobject.table;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.sql.DataSource;

import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.index.Index;
import com.openddal.dbobject.index.IndexCondition;
import com.openddal.dbobject.index.IndexType;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.table.PlanItem.ScanningStrategy;
import com.openddal.engine.Constants;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.repo.JdbcRepository;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.JdbcUtils;
import com.openddal.util.MathUtils;
import com.openddal.util.New;
import com.openddal.util.StringUtils;
import com.openddal.value.DataType;
import com.openddal.value.ValueDate;
import com.openddal.value.ValueTime;
import com.openddal.value.ValueTimestamp;

/**
 * @author jorgie.li
 */
public class TableMate extends Table {

    private static final int MAX_RETRY = 2;

    private final TableRule tableRule;
    private final ArrayList<Index> indexes = New.arrayList();
    private Column[] ruleColumns;

    private DbException initException;
    private boolean storesLowerCase;
    private boolean storesMixedCase;
    private boolean storesMixedCaseQuoted;
    private boolean supportsMixedCaseIdentifiers;

    public TableMate(Schema schema, String name, TableRule tableRule) {
        super(schema, name);
        this.tableRule = tableRule;
    }

    /**
     * @return the shardingColumns
     */
    public Column[] getRuleColumns() {
        check();
        return ruleColumns;
    }

    public TableRule getTableRule() {
        return tableRule;
    }

    public void check() {
        if (initException != null) {
            Column[] cols = {};
            setColumns(cols);
            indexes.clear();
            throw initException;
        }
    }

    public boolean isInited() {
        return initException == null;
    }

    public void markDeleted() {
        Column[] cols = {};
        setColumns(cols);
        indexes.clear();
        initException = DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, this.getSQL());
    }

    @Override
    public String getTableType() {
        return TABLE;
    }

    @Override
    public Index getUniqueIndex() {
        for (Index idx : indexes) {
            if (idx.getIndexType().isUnique()) {
                return idx;
            }
        }
        return null;
    }

    @Override
    public ArrayList<Index> getIndexes() {
        return indexes;
    }

    @Override
    public boolean isDeterministic() {
        return false;
    }

    @Override
    public long getRowCountApproximation() {
        return Constants.COST_ROW_OFFSET;
    }

    @Override
    public void checkRename() {

    }

    private Index addIndex(String name, ArrayList<Column> list, IndexType indexType) {
        Column[] cols = new Column[list.size()];
        list.toArray(cols);
        Index index = new Index(this, name, IndexColumn.wrap(cols), indexType);
        indexes.add(index);
        return index;
    }

    public PlanItem getBestPlanItem(Session session, int[] masks, TableFilter[] filters, int filter) {
        PlanItem item = new PlanItem();
        item.cost = Constants.COST_ROW_OFFSET;
        if (tableRule instanceof ShardedTableRule) {
            ShardedTableRule shardedTableRule = (ShardedTableRule) tableRule;
            int nodeCount = shardedTableRule.getObjectNodes().length;
            item.cost = Constants.COST_ROW_OFFSET * nodeCount;
        }

        Column[] columns = getRuleColumns();
        if (columns != null && masks != null) {
            for (int i = 0, len = columns.length; i < len; i++) {
                Column column = columns[i];
                int index = column.getColumnId();
                int mask = masks[index];
                if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                    if (i == columns.length - 1) {
                        item.cost = Constants.COST_ROW_OFFSET;
                        item.scanningStrategyFor(ScanningStrategy.USE_SHARDINGKEY);
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        double rowsCost = item.cost;
        ArrayList<Index> indexes = getIndexes();
        if (indexes != null && masks != null) {
            for (Index index : indexes) {
                columns = index.getColumns();
                for (int i = 0, len = columns.length; i < len; i++) {
                    Column column = columns[i];
                    int columnId = column.getColumnId();
                    int mask = masks[columnId];
                    if ((mask & IndexCondition.EQUALITY) == IndexCondition.EQUALITY) {
                        if (i == columns.length - 1 && index.getIndexType().isUnique()) {
                            item.cost = rowsCost * 0.25d;
                            item.scanningStrategyFor(ScanningStrategy.USE_UNIQUEKEY);
                            break;
                        }
                        item.cost = Math.max(rowsCost * 0.5d, item.cost * 0.5d);
                        item.scanningStrategyFor(ScanningStrategy.USE_INDEXKEY);
                    } else if ((mask & IndexCondition.RANGE) == IndexCondition.RANGE) {
                        item.cost = item.cost * 0.70d;
                        item.scanningStrategyFor(ScanningStrategy.USE_INDEXKEY);
                        break;
                    } else if ((mask & IndexCondition.START) == IndexCondition.START) {
                        item.cost = item.cost * 0.75d;
                        item.scanningStrategyFor(ScanningStrategy.USE_INDEXKEY);
                        break;
                    } else if ((mask & IndexCondition.END) == IndexCondition.END) {
                        item.cost = item.cost * 0.75d;
                        item.scanningStrategyFor(ScanningStrategy.USE_INDEXKEY);
                        break;
                    } else {
                        break;
                    }
                }
            }
        }
        //validationPlanItem(item);
        return item;
    }

    public void loadMataData(Session session) {
        ObjectNode node = tableRule.getMetadataNode();
        String tableName = node.getCompositeObjectName();
        String shardName = node.getShardName();
        try {
            trace.debug("Try to load {0} metadata from table {1}.{2}", getName(), shardName, tableName);
            readMataData(session, node);
            trace.debug("Load the {0} metadata success.", getName());
            initException = null;
        } catch (DbException e) {
            if(e.getErrorCode() == ErrorCode.COLUMN_NOT_FOUND_1) {
                throw e;
            }
            trace.debug("Fail to load {0} metadata from table {1}.{2}. error: {3}", getName(), shardName, tableName,
                    e.getCause().getMessage());
            initException = e;
            Column[] cols = {};
            setColumns(cols);
        }
        if (isInited()) {
            setRuleColumns();
        }
    }

    /**
     * @param session
     */
    public void readMataData(Session session, ObjectNode matadataNode) {
        for (int retry = 0;; retry++) {
            try {
                Connection conn = null;
                String shardName = matadataNode.getShardName();
                String tableName = matadataNode.getQualifiedObjectName();
                String catalog = matadataNode.getCatalog();
                String schema = matadataNode.getSchema();
                try {
                    JdbcRepository dsRepository = (JdbcRepository) database.getRepository();
                    DataSource dataSource = dsRepository.getDataSourceByShardName(shardName);
                    conn = dataSource.getConnection();
                    tableName = database.identifier(tableName);
                    if (catalog != null) {
                        catalog = database.identifier(catalog);
                    }
                    if (schema != null) {
                        schema = database.identifier(schema);
                    }
                    tryReadMetaData(conn, catalog, schema, tableName);
                    return;
                } catch (Exception e) {
                    throw DbException.convert(e);
                } finally {
                    JdbcUtils.closeSilently(conn);
                }
            } catch (DbException e) {
                if (retry >= MAX_RETRY) {
                    throw e;
                }
            }
        }

    }

    private void tryReadMetaData(Connection conn, String oCatalog, String oSchema, String tableName)
            throws SQLException {

        DatabaseMetaData meta = conn.getMetaData();
        storesLowerCase = meta.storesLowerCaseIdentifiers();
        storesMixedCase = meta.storesMixedCaseIdentifiers();
        storesMixedCaseQuoted = meta.storesMixedCaseQuotedIdentifiers();
        supportsMixedCaseIdentifiers = meta.supportsMixedCaseIdentifiers();

        ResultSet rs = meta.getTables(oCatalog, oSchema, tableName, null);
        if (rs.next() && rs.next()) {
            throw DbException.get(ErrorCode.SCHEMA_NAME_MUST_MATCH, tableName);
        }
        rs.close();
        rs = meta.getColumns(null, null, tableName, null);
        int i = 0;
        ArrayList<Column> columnList = New.arrayList();
        HashMap<String, Column> columnMap = New.hashMap();
        String catalog = null, schema = null;
        while (rs.next()) {
            String thisCatalog = rs.getString("TABLE_CAT");
            if (catalog == null) {
                catalog = thisCatalog;
            }
            String thisSchema = rs.getString("TABLE_SCHEM");
            if (schema == null) {
                schema = thisSchema;
            }
            if (!StringUtils.equals(catalog, thisCatalog) || !StringUtils.equals(schema, thisSchema)) {
                // if the table exists in multiple schemas or tables,
                // use the alternative solution
                columnMap.clear();
                columnList.clear();
                break;
            }
            String n = rs.getString("COLUMN_NAME");
            n = convertColumnName(n);
            int sqlType = rs.getInt("DATA_TYPE");
            long precision = rs.getInt("COLUMN_SIZE");
            precision = convertPrecision(sqlType, precision);
            int scale = rs.getInt("DECIMAL_DIGITS");
            scale = convertScale(sqlType, scale);
            int displaySize = MathUtils.convertLongToInt(precision);
            int type = DataType.convertSQLTypeToValueType(sqlType);
            Column col = new Column(n, type, precision, scale, displaySize);
            col.setTable(this, i++);
            columnList.add(col);
            columnMap.put(n, col);
        }
        rs.close();
        // check if the table is accessible
        Statement stat = null;
        try {
            stat = conn.createStatement();
            rs = stat.executeQuery("SELECT * FROM " + tableName + " T WHERE 1=0");
            if (columnList.size() == 0) {
                // alternative solution
                ResultSetMetaData rsMeta = rs.getMetaData();
                for (i = 0; i < rsMeta.getColumnCount();) {
                    String n = rsMeta.getColumnName(i + 1);
                    n = convertColumnName(n);
                    int sqlType = rsMeta.getColumnType(i + 1);
                    long precision = rsMeta.getPrecision(i + 1);
                    precision = convertPrecision(sqlType, precision);
                    int scale = rsMeta.getScale(i + 1);
                    scale = convertScale(sqlType, scale);
                    int displaySize = rsMeta.getColumnDisplaySize(i + 1);
                    int type = DataType.getValueTypeFromResultSet(rsMeta, i + 1);
                    Column col = new Column(n, type, precision, scale, displaySize);
                    col.setTable(this, i++);
                    columnList.add(col);
                    columnMap.put(n, col);
                }
            }
            rs.close();
        } catch (Exception e) {
            throw DbException.get(ErrorCode.TABLE_OR_VIEW_NOT_FOUND_1, e, tableName + "(" + e.toString() + ")");
        } finally {
            JdbcUtils.closeSilently(stat);
        }
        Column[] cols = new Column[columnList.size()];
        columnList.toArray(cols);
        setColumns(cols);
        // create scan index

        // load primary keys
        try {
            rs = meta.getPrimaryKeys(null, null, tableName);
        } catch (Exception e) {
            // Some ODBC bridge drivers don't support it:
            // some combinations of "DataDirect SequeLink(R) for JDBC"
            // http://www.datadirect.com/index.ssp
            rs = null;
        }
        String pkName = "";
        ArrayList<Column> list;
        if (rs != null && rs.next()) {
            // the problem is, the rows are not sorted by KEY_SEQ
            list = New.arrayList();
            do {
                int idx = rs.getInt("KEY_SEQ");
                if (pkName == null) {
                    pkName = rs.getString("PK_NAME");
                }
                while (list.size() < idx) {
                    list.add(null);
                }
                String col = rs.getString("COLUMN_NAME");
                col = convertColumnName(col);
                Column column = columnMap.get(col);
                if (idx == 0) {
                    // workaround for a bug in the SQLite JDBC driver
                    list.add(column);
                } else {
                    list.set(idx - 1, column);
                }
            } while (rs.next());
            addIndex(pkName, list, IndexType.createPrimaryKey(false));
            rs.close();
        }

        try {
            rs = meta.getIndexInfo(null, null, tableName, false, true);
        } catch (Exception e) {
            // Oracle throws an exception if the table is not found or is a
            // SYNONYM
            rs = null;
        }
        String indexName = null;
        list = New.arrayList();
        IndexType indexType = null;
        if (rs != null) {
            while (rs.next()) {
                if (rs.getShort("TYPE") == DatabaseMetaData.tableIndexStatistic) {
                    // ignore index statistics
                    continue;
                }
                String newIndex = rs.getString("INDEX_NAME");
                if (pkName.equals(newIndex)) {
                    continue;
                }
                if (indexName != null && !indexName.equals(newIndex)) {
                    addIndex(indexName, list, indexType);
                    indexName = null;
                }
                if (indexName == null) {
                    indexName = newIndex;
                    list.clear();
                }
                boolean unique = !rs.getBoolean("NON_UNIQUE");
                indexType = unique ? IndexType.createUnique(false) : IndexType.createNonUnique();
                String col = rs.getString("COLUMN_NAME");
                col = convertColumnName(col);
                Column column = columnMap.get(col);
                list.add(column);
            }
            rs.close();
        }
        if (indexName != null) {
            addIndex(indexName, list, indexType);
        }
        shardingKeyIndex();
    }

    private String convertColumnName(String columnName) {
        if ((storesMixedCase || storesLowerCase) && columnName.equals(StringUtils.toLowerEnglish(columnName))) {
            columnName = StringUtils.toUpperEnglish(columnName);
        } else if (storesMixedCase && !supportsMixedCaseIdentifiers) {
            // TeraData
            columnName = StringUtils.toUpperEnglish(columnName);
        } else if (storesMixedCase && storesMixedCaseQuoted) {
            // MS SQL Server (identifiers are case insensitive even if quoted)
            columnName = StringUtils.toUpperEnglish(columnName);
        }
        return columnName;
    }

    private void shardingKeyIndex() {
        // create shardingKey index
        if (ruleColumns != null) {
            ArrayList<Index> indexes = getIndexes();
            boolean isMatch = false;
            for (Index index : indexes) {
                Column[] columns = index.getColumns();
                if (columns.length != ruleColumns.length) {
                    continue;
                }
                boolean shardingKeyIndex = true;
                for (int idx = 0; idx < columns.length; idx++) {
                    if (columns[idx] != ruleColumns[idx]) {
                        shardingKeyIndex = false;
                        break;
                    }
                }
                if (shardingKeyIndex) {
                    index.getIndexType().shardingKeyIndex();
                    isMatch = true;
                }
            }
            if (!isMatch) {
                List<Column> asList = Arrays.asList(ruleColumns);
                addIndex("$shardingKey", New.arrayList(asList), IndexType.createShardingKey(false));
            }
        }
    }

    private static long convertPrecision(int sqlType, long precision) {
        // workaround for an Oracle problem:
        // for DATE columns, the reported precision is 7
        // for DECIMAL columns, the reported precision is 0
        switch (sqlType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            if (precision == 0) {
                precision = 65535;
            }
            break;
        case Types.DATE:
            precision = Math.max(ValueDate.PRECISION, precision);
            break;
        case Types.TIMESTAMP:
            precision = Math.max(ValueTimestamp.PRECISION, precision);
            break;
        case Types.TIME:
            precision = Math.max(ValueTime.PRECISION, precision);
            break;
        }
        return precision;
    }

    private static int convertScale(int sqlType, int scale) {
        // workaround for an Oracle problem:
        // for DECIMAL columns, the reported precision is -127
        switch (sqlType) {
        case Types.DECIMAL:
        case Types.NUMERIC:
            if (scale < 0) {
                scale = 32767;
            }
            break;
        }
        return scale;
    }

    /**
     * validation the rule columns is in the table columns
     */
    private void setRuleColumns() {
        if (tableRule instanceof ShardedTableRule) {
            ShardedTableRule shardedTableRule = (ShardedTableRule) tableRule;
            String[] ruleColNames = shardedTableRule.getRuleColumns();
            ruleColumns = new Column[ruleColNames.length];
            for (int i = 0; i < ruleColNames.length; i++) {
                String colName = database.identifier(ruleColNames[i]);
                if(!doesColumnExist(colName)) {
                    throw DbException.get(ErrorCode.SHARDING_COLUMN_NOT_FOUND,  colName ,getName());
                }
                ruleColumns[i] = getColumn(colName);
            }
        }
    }

    public void validationPlanItem(PlanItem item) {
        int priority = item.getScanningStrategy().priority;
        if (tableRule instanceof ShardedTableRule) {
            ShardedTableRule shardedTableRule = (ShardedTableRule) tableRule;
            switch (shardedTableRule.getScanLevel()) {
            case ShardedTableRule.SCANLEVEL_SHARDINGKEY:
                if (priority < ScanningStrategy.USE_SHARDINGKEY.priority) {
                    throw DbException.get(ErrorCode.ALLOWED_SCANTABLE_ERROR, getName(), "shardingKey", "shardingKey");
                }
                break;
            case ShardedTableRule.SCANLEVEL_UNIQUEINDEX:
                if (priority < ScanningStrategy.USE_UNIQUEKEY.priority) {
                    throw DbException.get(ErrorCode.ALLOWED_SCANTABLE_ERROR, getName(), "uniqueIndex", "uniqueIndex");
                }
                break;
            case ShardedTableRule.SCANLEVEL_ANYINDEX:
                if (priority < ScanningStrategy.USE_INDEXKEY.priority) {
                    throw DbException.get(ErrorCode.ALLOWED_SCANTABLE_ERROR, getName(), "indexKey", "indexKey");
                }
                break;
            case ShardedTableRule.SCANLEVEL_UNLIMITED:
                break;
            default:
                throw DbException.throwInternalError("invalid scanLevel");
            }
        }
    }
}
