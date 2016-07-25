package com.openddal.dbobject.table;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;

import com.openddal.command.Command;
import com.openddal.config.GlobalTableRule;
import com.openddal.config.ShardedTableRule;
import com.openddal.config.TableRule;
import com.openddal.dbobject.DbObject;
import com.openddal.dbobject.User;
import com.openddal.dbobject.index.Index;
import com.openddal.dbobject.index.IndexType;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.schema.SchemaObject;
import com.openddal.dbobject.schema.Sequence;
import com.openddal.engine.Constants;
import com.openddal.engine.Database;
import com.openddal.engine.QueryStatisticsData;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.result.Csv;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.result.SortOrder;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.MathUtils;
import com.openddal.util.New;
import com.openddal.util.StatementBuilder;
import com.openddal.util.StringUtils;
import com.openddal.util.Utils;
import com.openddal.value.CompareMode;
import com.openddal.value.DataType;
import com.openddal.value.Value;
import com.openddal.value.ValueNull;
import com.openddal.value.ValueString;
import com.openddal.value.ValueStringIgnoreCase;

/**
 * This class is responsible to build the database meta data pseudo tables.
 */
public class MetaTable extends Table {

    /**
     * The approximate number of rows of a meta table.
     */
    public static final long ROW_COUNT_APPROXIMATION = 1000;

    private static final String CHARACTER_SET_NAME = "Unicode";

    private static final int TABLES = 0;
    private static final int COLUMNS = 1;
    private static final int INDEXES = 2;
    private static final int TABLE_TYPES = 3;
    private static final int TYPE_INFO = 4;
    private static final int CATALOGS = 5;
    private static final int SETTINGS = 6;
    private static final int HELP = 7;
    private static final int SEQUENCES = 8;
    private static final int USERS = 9;
    private static final int ROLES = 10;
    private static final int RIGHTS = 11;
    private static final int FUNCTION_ALIASES = 12;
    private static final int SCHEMATA = 13;
    private static final int TABLE_PRIVILEGES = 14;
    private static final int COLUMN_PRIVILEGES = 15;
    private static final int COLLATIONS = 16;
    private static final int VIEWS = 17;
    private static final int IN_DOUBT = 18;
    private static final int CROSS_REFERENCES = 19;
    private static final int CONSTRAINTS = 20;
    private static final int FUNCTION_COLUMNS = 21;
    private static final int CONSTANTS = 22;
    private static final int PARTITIONS = 23;
    private static final int TRIGGERS = 24;
    private static final int SESSIONS = 25;
    private static final int LOCKS = 26;
    private static final int SESSION_STATE = 27;
    private static final int QUERY_STATISTICS = 28;
    private static final int META_TABLE_TYPE_COUNT = QUERY_STATISTICS + 1;

    private final int type;
    private final int indexColumn;

    /**
     * Create a new metadata table.
     *
     * @param schema the schema
     * @param id the object id
     * @param type the meta table type
     */
    public MetaTable(Schema schema, int type) {
        // tableName will be set later
        super(schema, null);
        this.type = type;
        Column[] cols;
        String indexColumnName = null;
        switch (type) {
        case TABLES:
            setObjectName("TABLES");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "TABLE_TYPE",
                    // extensions
                    "STORAGE_TYPE",
                    "SQL",
                    "REMARKS",
                    "LAST_MODIFICATION BIGINT",
                    "ID INT",
                    "TYPE_NAME",
                    "TABLE_CLASS",
                    "ROW_COUNT_ESTIMATE BIGINT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case COLUMNS:
            setObjectName("COLUMNS");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "ORDINAL_POSITION INT",
                    "COLUMN_DEFAULT",
                    "IS_NULLABLE",
                    "DATA_TYPE INT",
                    "CHARACTER_MAXIMUM_LENGTH INT",
                    "CHARACTER_OCTET_LENGTH INT",
                    "NUMERIC_PRECISION INT",
                    "NUMERIC_PRECISION_RADIX INT",
                    "NUMERIC_SCALE INT",
                    "CHARACTER_SET_NAME",
                    "COLLATION_NAME",
                    // extensions
                    "TYPE_NAME",
                    "NULLABLE INT",
                    "IS_COMPUTED BIT",
                    "SELECTIVITY INT",
                    "CHECK_CONSTRAINT",
                    "SEQUENCE_NAME",
                    "REMARKS",
                    "SOURCE_DATA_TYPE SMALLINT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case INDEXES:
            setObjectName("INDEXES");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "NON_UNIQUE BIT",
                    "INDEX_NAME",
                    "ORDINAL_POSITION SMALLINT",
                    "COLUMN_NAME",
                    "CARDINALITY INT",
                    "PRIMARY_KEY BIT",
                    "INDEX_TYPE_NAME",
                    "IS_GENERATED BIT",
                    "INDEX_TYPE SMALLINT",
                    "ASC_OR_DESC",
                    "PAGES INT",
                    "FILTER_CONDITION",
                    "REMARKS",
                    "SQL",
                    "ID INT",
                    "SORT_TYPE INT",
                    "CONSTRAINT_NAME",
                    "INDEX_CLASS"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case TABLE_TYPES:
            setObjectName("TABLE_TYPES");
            cols = createColumns("TYPE");
            break;
        case TYPE_INFO:
            setObjectName("TYPE_INFO");
            cols = createColumns(
                "TYPE_NAME",
                "DATA_TYPE INT",
                "PRECISION INT",
                "PREFIX",
                "SUFFIX",
                "PARAMS",
                "AUTO_INCREMENT BIT",
                "MINIMUM_SCALE SMALLINT",
                "MAXIMUM_SCALE SMALLINT",
                "RADIX INT",
                "POS INT",
                "CASE_SENSITIVE BIT",
                "NULLABLE SMALLINT",
                "SEARCHABLE SMALLINT"
            );
            break;
        case CATALOGS:
            setObjectName("CATALOGS");
            cols = createColumns("CATALOG_NAME");
            break;
        case SETTINGS:
            setObjectName("SETTINGS");
            cols = createColumns("NAME", "VALUE");
            break;
        case HELP:
            setObjectName("HELP");
            cols = createColumns(
                    "ID INT",
                    "SECTION",
                    "TOPIC",
                    "SYNTAX",
                    "TEXT"
            );
            break;
        case SEQUENCES:
            setObjectName("SEQUENCES");
            cols = createColumns(
                    "SEQUENCE_CATALOG",
                    "SEQUENCE_SCHEMA",
                    "SEQUENCE_NAME",
                    "CURRENT_VALUE BIGINT",
                    "INCREMENT BIGINT",
                    "IS_GENERATED BIT",
                    "REMARKS",
                    "CACHE BIGINT",
                    "MIN_VALUE BIGINT",
                    "MAX_VALUE BIGINT",
                    "IS_CYCLE BIT",
                    "ID INT"
            );
            break;
        case USERS:
            setObjectName("USERS");
            cols = createColumns(
                    "NAME",
                    "ADMIN",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case ROLES:
            setObjectName("ROLES");
            cols = createColumns(
                    "NAME",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case RIGHTS:
            setObjectName("RIGHTS");
            cols = createColumns(
                    "GRANTEE",
                    "GRANTEETYPE",
                    "GRANTEDROLE",
                    "RIGHTS",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "ID INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case FUNCTION_ALIASES:
            setObjectName("FUNCTION_ALIASES");
            cols = createColumns(
                    "ALIAS_CATALOG",
                    "ALIAS_SCHEMA",
                    "ALIAS_NAME",
                    "JAVA_CLASS",
                    "JAVA_METHOD",
                    "DATA_TYPE INT",
                    "TYPE_NAME",
                    "COLUMN_COUNT INT",
                    "RETURNS_RESULT SMALLINT",
                    "REMARKS",
                    "ID INT",
                    "SOURCE"
            );
            break;
        case FUNCTION_COLUMNS:
            setObjectName("FUNCTION_COLUMNS");
            cols = createColumns(
                    "ALIAS_CATALOG",
                    "ALIAS_SCHEMA",
                    "ALIAS_NAME",
                    "JAVA_CLASS",
                    "JAVA_METHOD",
                    "COLUMN_COUNT INT",
                    "POS INT",
                    "COLUMN_NAME",
                    "DATA_TYPE INT",
                    "TYPE_NAME",
                    "PRECISION INT",
                    "SCALE SMALLINT",
                    "RADIX SMALLINT",
                    "NULLABLE SMALLINT",
                    "COLUMN_TYPE SMALLINT",
                    "REMARKS",
                    "COLUMN_DEFAULT"
            );
            break;
        case SCHEMATA:
            setObjectName("SCHEMATA");
            cols = createColumns(
                    "CATALOG_NAME",
                    "SCHEMA_NAME",
                    "SCHEMA_OWNER",
                    "DEFAULT_CHARACTER_SET_NAME",
                    "DEFAULT_COLLATION_NAME",
                    "IS_DEFAULT BIT",
                    "REMARKS",
                    "ID INT"
            );
            break;
        case TABLE_PRIVILEGES:
            setObjectName("TABLE_PRIVILEGES");
            cols = createColumns(
                    "GRANTOR",
                    "GRANTEE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "PRIVILEGE_TYPE",
                    "IS_GRANTABLE"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case COLUMN_PRIVILEGES:
            setObjectName("COLUMN_PRIVILEGES");
            cols = createColumns(
                    "GRANTOR",
                    "GRANTEE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "COLUMN_NAME",
                    "PRIVILEGE_TYPE",
                    "IS_GRANTABLE"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case COLLATIONS:
            setObjectName("COLLATIONS");
            cols = createColumns(
                    "NAME",
                    "KEY"
            );
            break;
        case VIEWS:
            setObjectName("VIEWS");
            cols = createColumns(
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "VIEW_DEFINITION",
                    "CHECK_OPTION",
                    "IS_UPDATABLE",
                    "STATUS",
                    "REMARKS",
                    "ID INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case IN_DOUBT:
            setObjectName("IN_DOUBT");
            cols = createColumns(
                    "TRANSACTION",
                    "STATE"
            );
            break;
        case CROSS_REFERENCES:
            setObjectName("CROSS_REFERENCES");
            cols = createColumns(
                    "PKTABLE_CATALOG",
                    "PKTABLE_SCHEMA",
                    "PKTABLE_NAME",
                    "PKCOLUMN_NAME",
                    "FKTABLE_CATALOG",
                    "FKTABLE_SCHEMA",
                    "FKTABLE_NAME",
                    "FKCOLUMN_NAME",
                    "ORDINAL_POSITION SMALLINT",
                    "UPDATE_RULE SMALLINT",
                    "DELETE_RULE SMALLINT",
                    "FK_NAME",
                    "PK_NAME",
                    "DEFERRABILITY SMALLINT"
            );
            indexColumnName = "PKTABLE_NAME";
            break;
        case CONSTRAINTS:
            setObjectName("CONSTRAINTS");
            cols = createColumns(
                    "CONSTRAINT_CATALOG",
                    "CONSTRAINT_SCHEMA",
                    "CONSTRAINT_NAME",
                    "CONSTRAINT_TYPE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "UNIQUE_INDEX_NAME",
                    "CHECK_EXPRESSION",
                    "COLUMN_LIST",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            indexColumnName = "TABLE_NAME";
            break;
        case CONSTANTS:
            setObjectName("CONSTANTS");
            cols = createColumns(
                    "CONSTANT_CATALOG",
                    "CONSTANT_SCHEMA",
                    "CONSTANT_NAME",
                    "DATA_TYPE INT",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            break;
        case PARTITIONS:
            setObjectName("PARTITIONS");
            cols = createColumns(
                    "PARTITIONS_CATALOG", 
                    "PARTITIONS_SCHEMA", 
                    "OBJECT_NAME", 
                    "DATA_NODE", 
                    "NODE_NAME", 
                    "NODE_TYPE",
                    "PARTITIONER");
            break;
        case TRIGGERS:
            setObjectName("TRIGGERS");
            cols = createColumns(
                    "TRIGGER_CATALOG",
                    "TRIGGER_SCHEMA",
                    "TRIGGER_NAME",
                    "TRIGGER_TYPE",
                    "TABLE_CATALOG",
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "BEFORE BIT",
                    "JAVA_CLASS",
                    "QUEUE_SIZE INT",
                    "NO_WAIT BIT",
                    "REMARKS",
                    "SQL",
                    "ID INT"
            );
            break;
        case SESSIONS: {
            setObjectName("SESSIONS");
            cols = createColumns(
                    "ID INT",
                    "USER_NAME",
                    "SESSION_START",
                    "STATEMENT",
                    "STATEMENT_START",
                    "CONTAINS_UNCOMMITTED"
            );
            break;
        }
        case LOCKS: {
            setObjectName("LOCKS");
            cols = createColumns(
                    "TABLE_SCHEMA",
                    "TABLE_NAME",
                    "SESSION_ID INT",
                    "LOCK_TYPE"
            );
            break;
        }
        case SESSION_STATE: {
            setObjectName("SESSION_STATE");
            cols = createColumns(
                    "KEY",
                    "SQL"
            );
            break;
        }
        case QUERY_STATISTICS: {
            setObjectName("QUERY_STATISTICS");
            cols = createColumns(
                    "SQL_STATEMENT",
                    "EXECUTION_COUNT INT",
                    "MIN_EXECUTION_TIME LONG",
                    "MAX_EXECUTION_TIME LONG",
                    "CUMULATIVE_EXECUTION_TIME LONG",
                    "AVERAGE_EXECUTION_TIME DOUBLE",
                    "STD_DEV_EXECUTION_TIME DOUBLE",
                    "MIN_ROW_COUNT INT",
                    "MAX_ROW_COUNT INT",
                    "CUMULATIVE_ROW_COUNT LONG",
                    "AVERAGE_ROW_COUNT DOUBLE",
                    "STD_DEV_ROW_COUNT DOUBLE"
            );
            break;
        }
        default:
            throw DbException.throwInternalError("type="+type);
        }
        setColumns(cols);

        if (indexColumnName == null) {
            indexColumn = -1;
        } else {
            indexColumn = getColumn(indexColumnName).getColumnId();
        }
    }

    private Column[] createColumns(String... names) {
        Column[] cols = new Column[names.length];
        for (int i = 0; i < names.length; i++) {
            String nameType = names[i];
            int idx = nameType.indexOf(' ');
            int dataType;
            String name;
            if (idx < 0) {
                dataType = database.getMode().lowerCaseIdentifiers ?
                        Value.STRING_IGNORECASE : Value.STRING;
                name = nameType;
            } else {
                dataType = DataType.getTypeByName(nameType.substring(idx + 1)).type;
                name = nameType.substring(0, idx);
            }
            cols[i] = new Column(name, dataType);
        }
        return cols;
    }




    private String identifier(String s) {
        if (database.getMode().lowerCaseIdentifiers) {
            s = s == null ? null : StringUtils.toLowerEnglish(s);
        }
        return s;
    }

    private ArrayList<Table> getAllTables(Session session) {
        ArrayList<Table> tables = database.getAllTablesAndViews();
        ArrayList<Table> tempTables = session.getLocalTempTables();
        tables.addAll(tempTables);
        return tables;
    }

    private ArrayList<Table> getTablesByName(Session session, String tableName) {
        if (database.getMode().lowerCaseIdentifiers) {
            tableName = StringUtils.toUpperEnglish(tableName);
        }
        ArrayList<Table> tables = database.getTableOrViewByName(tableName);
        for (Table temp : session.getLocalTempTables()) {
            if (temp.getName().equals(tableName)) {
                tables.add(temp);
            }
        }
        return tables;
    }

    private boolean checkIndex(Session session, String value, Value indexFrom,
            Value indexTo) {
        if (value == null || (indexFrom == null && indexTo == null)) {
            return true;
        }
        Database db = session.getDatabase();
        Value v;
        if (database.getMode().lowerCaseIdentifiers) {
            v = ValueStringIgnoreCase.get(value);
        } else {
            v = ValueString.get(value);
        }
        if (indexFrom != null && db.compare(v, indexFrom) < 0) {
            return false;
        }
        if (indexTo != null && db.compare(v, indexTo) > 0) {
            return false;
        }
        return true;
    }

    private static String replaceNullWithEmpty(String s) {
        return s == null ? "" : s;
    }

    /**
     * Generate the data for the given metadata table using the given first and
     * last row filters.
     *
     * @param session the session
     * @param first the first row to return
     * @param last the last row to return
     * @return the generated rows
     */
    public ArrayList<Row> generateRows(Session session, SearchRow first,
            SearchRow last) {
        Value indexFrom = null, indexTo = null;

        if (indexColumn >= 0) {
            if (first != null) {
                indexFrom = first.getValue(indexColumn);
            }
            if (last != null) {
                indexTo = last.getValue(indexColumn);
            }
        }

        ArrayList<Row> rows = New.arrayList();
        String catalog = "";/*identifier(database.getShortName())*/;
        boolean admin = session.getUser().isAdmin();
        switch (type) {
        case TABLES: {
            for (Table table : getAllTables(session)) {
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                String storageType = table.getTableType();
                if(table instanceof TableMate) {
                    int type2 = ((TableMate) table).getTableRule().getType();
                    switch (type2) {
                    case TableRule.FIXED_NODE_TABLE:
                        storageType = "FIXED_NODE_TABLE";
                        break;
                    case TableRule.GLOBAL_NODE_TABLE:
                        storageType = "GLOBAL_NODE_TABLE";
                        break;
                    case TableRule.SHARDED_NODE_TABLE:
                        storageType = "SHARDED_NODE_TABLE";
                        break;

                    default:
                        break;
                    }
                }
                add(rows,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
                        // TABLE_NAME
                        tableName,
                        // TABLE_TYPE
                        table.getTableType(),
                        // STORAGE_TYPE
                        storageType,
                        // SQL
                        "",
                        // REMARKS
                        "",
                        // LAST_MODIFICATION
                        //"",
                        // Type BIGINT: optional value should be null
                        // @author little-pan
                        // @since 2016-07-13
                        null,
                        // ID
                        "" + table.getId(),
                        // TYPE_NAME
                        null,
                        // TABLE_CLASS
                        table.getClass().getName(),
                        // ROW_COUNT_ESTIMATE
                        "" + table.getRowCountApproximation()
                );
            }
            break;
        }
        case COLUMNS: {
            // reduce the number of tables to scan - makes some metadata queries
            // 10x faster
            final ArrayList<Table> tablesToList;
            if (indexFrom != null && indexTo != null && indexFrom.equals(indexTo)) {
                String tableName = identifier(indexFrom.getString());
                tablesToList = getTablesByName(session, tableName);
            } else {
                tablesToList = getAllTables(session);
            }
            for (Table table : tablesToList) {
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                Column[] cols = table.getColumns();
                String collation = database.getCompareMode().getName();
                for (int j = 0; j < cols.length; j++) {
                    Column c = cols[j];
                    Sequence sequence = c.getSequence();
                    add(rows,
                            // TABLE_CATALOG
                            catalog,
                            // TABLE_SCHEMA
                            identifier(table.getSchema().getName()),
                            // TABLE_NAME
                            tableName,
                            // COLUMN_NAME
                            identifier(c.getName()),
                            // ORDINAL_POSITION
                            String.valueOf(j + 1),
                            // COLUMN_DEFAULT
                            c.getDefaultSQL(),
                            // IS_NULLABLE
                            c.isNullable() ? "YES" : "NO",
                            // DATA_TYPE
                            "" + DataType.convertTypeToSQLType(c.getType()),
                            // CHARACTER_MAXIMUM_LENGTH
                            "" + c.getPrecisionAsInt(),
                            // CHARACTER_OCTET_LENGTH
                            "" + c.getPrecisionAsInt(),
                            // NUMERIC_PRECISION
                            "" + c.getPrecisionAsInt(),
                            // NUMERIC_PRECISION_RADIX
                            "10",
                            // NUMERIC_SCALE
                            "" + c.getScale(),
                            // CHARACTER_SET_NAME
                            CHARACTER_SET_NAME,
                            // COLLATION_NAME
                            collation,
                            // TYPE_NAME
                            identifier(DataType.getDataType(c.getType()).name),
                            // NULLABLE
                            "" + (c.isNullable() ?
                                    DatabaseMetaData.columnNullable :
                                    DatabaseMetaData.columnNoNulls) ,
                            // IS_COMPUTED
                            "" + (c.getComputed() ? "TRUE" : "FALSE"),
                            // SELECTIVITY
                            "" + (c.getSelectivity()),
                            // CHECK_CONSTRAINT
                            c.getCheckConstraintSQL(session, c.getName()),
                            // SEQUENCE_NAME
                            sequence == null ? null : sequence.getName(),
                            // REMARKS
                            replaceNullWithEmpty(c.getComment()),
                            // SOURCE_DATA_TYPE
                            null
                    );
                }
            }
            break;
        }
        case INDEXES: {
            // reduce the number of tables to scan - makes some metadata queries
            // 10x faster
            final ArrayList<Table> tablesToList;
            if (indexFrom != null && indexTo != null && indexFrom.equals(indexTo)) {
                String tableName = identifier(indexFrom.getString());
                tablesToList = getTablesByName(session, tableName);
            } else {
                tablesToList = getAllTables(session);
            }
            for (Table table : tablesToList) {
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                ArrayList<Index> indexes = table.getIndexes();
                for (int j = 0; indexes != null && j < indexes.size(); j++) {
                    Index index = indexes.get(j);
                    IndexColumn[] cols = index.getIndexColumns();
                    for (int k = 0; k < cols.length; k++) {
                        IndexColumn idxCol = cols[k];
                        Column column = idxCol.column;
                        add(rows,
                                // TABLE_CATALOG
                                catalog,
                                // TABLE_SCHEMA
                                identifier(table.getSchema().getName()),
                                // TABLE_NAME
                                tableName,
                                // NON_UNIQUE
                                index.getIndexType().isUnique() ?
                                        "FALSE" : "TRUE",
                                // INDEX_NAME
                                identifier(index.getName()),
                                // ORDINAL_POSITION
                                "" + (k+1),
                                // COLUMN_NAME
                                identifier(column.getName()),
                                // CARDINALITY
                                "0",
                                // PRIMARY_KEY
                                index.getIndexType().isPrimaryKey() ?
                                        "TRUE" : "FALSE",
                                // INDEX_TYPE_NAME
                                index.getIndexType().getSQL(),
                                // IS_GENERATED
                                null,
                                // INDEX_TYPE
                                "" + DatabaseMetaData.tableIndexOther,
                                // ASC_OR_DESC
                                (idxCol.sortType & SortOrder.DESCENDING) != 0 ?
                                        "D" : "A",
                                // PAGES
                                "0",
                                // FILTER_CONDITION
                                "",
                                // REMARKS
                                "",
                                // SQL
                                "",
                                // ID
                                "" + index.getId(),
                                // SORT_TYPE
                                "" + idxCol.sortType,
                                // CONSTRAINT_NAME
                                "",
                                // INDEX_CLASS
                                ""
                            );
                    }
                }
            }
            break;
        }
        case TABLE_TYPES: {
            add(rows, Table.TABLE);
            add(rows, Table.SYSTEM_TABLE);
            add(rows, Table.VIEW);
            break;
        }
        case CATALOGS: {
            add(rows, catalog);
            break;
        }
        case SETTINGS: {
            add(rows, "info.BUILD_ID", "" + Constants.BUILD_ID);
            add(rows, "info.VERSION_MAJOR", "" + Constants.VERSION_MAJOR);
            add(rows, "info.VERSION_MINOR", "" + Constants.VERSION_MINOR);
            add(rows, "info.VERSION", "" + Constants.getFullVersion());
            if (admin) {
                String[] settings = {
                        "java.runtime.version", "java.vm.name",
                        "java.vendor", "os.name", "os.arch", "os.version",
                        "sun.os.patch.level", "file.separator",
                        "path.separator", "line.separator", "user.country",
                        "user.language", "user.variant", "file.encoding"                };
                for (String s : settings) {
                    add(rows, "property." + s, Utils.getProperty(s, ""));
                }
            }

            add(rows, "MODE", database.getMode().getName());
            add(rows, "QUERY_TIMEOUT", "" + session.getQueryTimeout());
            // database settings
            ArrayList<String> settingNames = New.arrayList();
            HashMap<String, String> s = database.getSettings().getSettings();
            for (String k : s.keySet()) {
                settingNames.add(k);
            }
            Collections.sort(settingNames);
            for (String k : settingNames) {
                add(rows, k, s.get(k));
            }
            break;
        }
        case TYPE_INFO: {
            for (DataType t : DataType.getTypes()) {
                if (t.hidden || t.sqlType == Value.NULL) {
                    continue;
                }
                add(rows,
                        // TYPE_NAME
                        t.name,
                        // DATA_TYPE
                        String.valueOf(t.sqlType),
                        // PRECISION
                        String.valueOf(MathUtils.convertLongToInt(t.maxPrecision)),
                        // PREFIX
                        t.prefix,
                        // SUFFIX
                        t.suffix,
                        // PARAMS
                        t.params,
                        // AUTO_INCREMENT
                        String.valueOf(t.autoIncrement),
                        // MINIMUM_SCALE
                        String.valueOf(t.minScale),
                        // MAXIMUM_SCALE
                        String.valueOf(t.maxScale),
                        // RADIX
                        t.decimal ? "10" : null,
                        // POS
                        String.valueOf(t.sqlTypePos),
                        // CASE_SENSITIVE
                        String.valueOf(t.caseSensitive),
                        // NULLABLE
                        "" + DatabaseMetaData.typeNullable,
                        // SEARCHABLE
                        "" + DatabaseMetaData.typeSearchable
                );
            }
            break;
        }
        case HELP: {
            String resource = "/help.csv";
            try {
                byte[] data = Utils.getResource(resource);
                Reader reader = new InputStreamReader(
                        new ByteArrayInputStream(data));
                Csv csv = new Csv();
                csv.setLineCommentCharacter('#');
                ResultSet rs = csv.read(reader, null);
                for (int i = 0; rs.next(); i++) {
                    add(rows,
                        // ID
                        String.valueOf(i),
                        // SECTION
                        rs.getString(1).trim(),
                        // TOPIC
                        rs.getString(2).trim(),
                        // SYNTAX
                        rs.getString(3).trim(),
                        // TEXT
                        rs.getString(4).trim()
                    );
                }
            } catch (Exception e) {
                throw DbException.convert(e);
            }
            break;
        }
        case SEQUENCES: {
            for (SchemaObject obj : database.getAllSchemaObjects(
                    DbObject.SEQUENCE)) {
                Sequence s = (Sequence) obj;
                add(rows,
                        // SEQUENCE_CATALOG
                        catalog,
                        // SEQUENCE_SCHEMA
                        identifier(s.getSchema().getName()),
                        // SEQUENCE_NAME
                        identifier(s.getName()),
                        // CURRENT_VALUE
                        String.valueOf(s.getCurrentValue()),
                        // INCREMENT
                        String.valueOf(s.getIncrement()),
                        // IS_GENERATED
                        s.getBelongsToTable() ? "TRUE" : "FALSE",
                        // REMARKS
                        "",
                        // CACHE
                        String.valueOf(s.getCacheSize()),
                        // MIN_VALUE
                        String.valueOf(s.getMinValue()),
                        // MAX_VALUE
                        String.valueOf(s.getMaxValue()),
                        // IS_CYCLE
                        s.getCycle() ? "TRUE" : "FALSE",
                        // ID
                        "" + s.getId()
                    );
            }
            break;
        }
        case USERS: {
            for (User u : database.getAllUsers()) {
                if (admin || session.getUser() == u) {
                    add(rows,
                            // NAME
                            identifier(u.getName()),
                            // ADMIN
                            String.valueOf(u.isAdmin()),
                            // REMARKS
                            "",
                            // ID
                            "" + u.getId()
                    );
                }
            }
            break;
        }
        case ROLES: {
            break;
        }
        case RIGHTS: {
            break;
        }
        case FUNCTION_ALIASES: {
            break;
        }
        case FUNCTION_COLUMNS: {
            break;
        }
        case SCHEMATA: {
            String collation = database.getCompareMode().getName();
            for (Schema schema : database.getAllSchemas()) {
                add(rows,
                        // CATALOG_NAME
                        catalog,
                        // SCHEMA_NAME
                        identifier(schema.getName()),
                        // SCHEMA_OWNER
                        identifier(schema.getOwner().getName()),
                        // DEFAULT_CHARACTER_SET_NAME
                        CHARACTER_SET_NAME,
                        // DEFAULT_COLLATION_NAME
                        collation,
                        // IS_DEFAULT
                        Constants.SCHEMA_MAIN.equals(
                                schema.getName()) ? "TRUE" : "FALSE",
                        // REMARKS
                        "",
                        // ID
                        "" + schema.getId()
                );
            }
            break;
        }
        case TABLE_PRIVILEGES: {
            
        }
        case COLUMN_PRIVILEGES: {
            
        }
        case COLLATIONS: {
            for (Locale l : Collator.getAvailableLocales()) {
                add(rows,
                        // NAME
                        CompareMode.getName(l),
                        // KEY
                        l.toString()
                );
            }
            break;
        }
        case VIEWS: {
            for (Table table : getAllTables(session)) {
                if (!table.getTableType().equals(Table.VIEW)) {
                    continue;
                }
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                TableView view = (TableView) table;
                add(rows,
                        // TABLE_CATALOG
                        catalog,
                        // TABLE_SCHEMA
                        identifier(table.getSchema().getName()),
                        // TABLE_NAME
                        tableName,
                        // VIEW_DEFINITION
                        "",
                        // CHECK_OPTION
                        "NONE",
                        // IS_UPDATABLE
                        "NO",
                        // STATUS
                        view.isInvalid() ? "INVALID" : "VALID",
                        // REMARKS
                        "",
                        // ID
                        "" + view.getId()
                );
            }
            break;
        }
        case SESSIONS: {
            long now = System.currentTimeMillis();
            for (Session s : database.getSessions()) {
                if (admin || s == session) {
                    Command command = s.getCurrentCommand();
                    long start = s.getCurrentCommandStart();
                    if (start == 0) {
                        start = now;
                    }
                    add(rows,
                            // ID
                            "" + s.getId(),
                            // USER_NAME
                            s.getUser().getName(),
                            // SESSION_START
                            new Timestamp(s.getSessionStart()).toString(),
                            // STATEMENT
                            command == null ? null : command.toString(),
                            // STATEMENT_START
                            new Timestamp(start).toString(),
                            // CONTAINS_UNCOMMITTED
                            "" + s.isReadOnly()
                    );
                }
            }
            break;
        }
        case SESSION_STATE: {
            for (String name : session.getVariableNames()) {
                Value v = session.getVariable(name);
                add(rows,
                        // KEY
                        "@" + name,
                        // SQL
                        "SET @" + name + " " + v.getSQL()
                );
            }
            for (Table table : session.getLocalTempTables()) {
                add(rows,
                        // KEY
                        "TABLE " + table.getName(),
                        // SQL
                        ""
                );
            }
            String[] path = session.getSchemaSearchPath();
            if (path != null && path.length > 0) {
                StatementBuilder buff = new StatementBuilder(
                        "SET SCHEMA_SEARCH_PATH ");
                for (String p : path) {
                    buff.appendExceptFirst(", ");
                    buff.append(StringUtils.quoteIdentifier(p));
                }
                add(rows,
                        // KEY
                        "SCHEMA_SEARCH_PATH",
                        // SQL
                        buff.toString()
                );
            }
            String schema = session.getCurrentSchemaName();
            if (schema != null) {
                add(rows,
                        // KEY
                        "SCHEMA",
                        // SQL
                        "SET SCHEMA " + StringUtils.quoteIdentifier(schema)
                );
            }
            break;
        }
        case QUERY_STATISTICS: {
            QueryStatisticsData control = database.getQueryStatisticsData();
            if (control != null) {
                for (QueryStatisticsData.QueryEntry entry : control.getQueries()) {
                    add(rows,
                            // SQL_STATEMENT
                            entry.sqlStatement,
                            // EXECUTION_COUNT
                            "" + entry.count,
                            // MIN_EXECUTION_TIME
                            "" + entry.executionTimeMin,
                            // MAX_EXECUTION_TIME
                            "" + entry.executionTimeMax,
                            // CUMULATIVE_EXECUTION_TIME
                            "" + entry.executionTimeCumulative,
                            // AVERAGE_EXECUTION_TIME
                            "" + entry.executionTimeMean,
                            // STD_DEV_EXECUTION_TIME
                            "" + entry.getExecutionTimeStandardDeviation(),
                            // MIN_ROW_COUNT
                            "" + entry.rowCountMin,
                            // MAX_ROW_COUNT
                            "" + entry.rowCountMax,
                            // CUMULATIVE_ROW_COUNT
                            "" + entry.rowCountCumulative,
                            // AVERAGE_ROW_COUNT
                            "" + entry.rowCountMean,
                            // STD_DEV_ROW_COUNT
                            "" + entry.getRowCountStandardDeviation()
                    );
                }
            }
            break;
        }
        case PARTITIONS: {
            for (Table table : getAllTables(session)) {
                String tableName = identifier(table.getName());
                if (!checkIndex(session, tableName, indexFrom, indexTo)) {
                    continue;
                }
                if (table instanceof TableMate) {
                    TableMate tableMate = (TableMate) table;
                    TableRule tableRule = tableMate.getTableRule();
                    int type2 = tableRule.getType();
                    switch (type2) {
                    case TableRule.FIXED_NODE_TABLE:
                        add(rows,
                                // PARTITIONS_CATALOG
                                catalog,
                                // PARTITIONS_SCHEMA
                                identifier(table.getSchema().getName()),
                                // OBJECT_NAME
                                tableName,
                                // DATA_NODE
                                tableRule.getMetadataNode().getShardName(),
                                // NODE_NAME
                                tableRule.getMetadataNode().getCompositeObjectName(),
                                // NODE_TYPE
                                "fixed",
                                // PARTITIONER
                                "");
                        break;
                    case TableRule.GLOBAL_NODE_TABLE:
                        GlobalTableRule globalRule = (GlobalTableRule) tableMate.getTableRule();
                        for (ObjectNode i : globalRule.getBroadcasts()) {
                            add(rows,
                                    // PARTITIONS_CATALOG
                                    catalog,
                                    // PARTITIONS_SCHEMA
                                    identifier(table.getSchema().getName()),
                                    // OBJECT_NAME
                                    tableName,
                                    // DATA_NODE
                                    i.getShardName(),
                                    // NODE_NAME
                                    i.getCompositeObjectName(),
                                    // NODE_TYPE
                                    "broadcast",
                                    // PARTITIONER
                                    "");
                        }
                        break;
                    case TableRule.SHARDED_NODE_TABLE:
                        ShardedTableRule shardRule = (ShardedTableRule) tableMate.getTableRule();
                        for (ObjectNode i : shardRule.getObjectNodes()) {
                            add(rows,
                                    // PARTITIONS_CATALOG
                                    catalog,
                                    // PARTITIONS_SCHEMA
                                    identifier(table.getSchema().getName()),
                                    // OBJECT_NAME
                                    tableName,
                                    // DATA_NODE
                                    i.getShardName(),
                                    // NODE_NAME
                                    i.getCompositeObjectName(),
                                    // NODE_TYPE
                                    "sharded",
                                    // PARTITIONER
                                    shardRule.getPartitioner().getClass().getName());
                        }
                        break;

                    default:
                        break;
                    }

                }
            }
            break;

        }
        case IN_DOUBT:
        case CROSS_REFERENCES:
        case CONSTRAINTS:
        case CONSTANTS: 
        case TRIGGERS:
        case LOCKS:
            break;
        default:
            DbException.throwInternalError("type="+type);
        }
        return rows;
    }


    private void add(ArrayList<Row> rows, String... strings) {
        Value[] values = new Value[strings.length];
        for (int i = 0; i < strings.length; i++) {
            String s = strings[i];
            Value v = (s == null) ? (Value) ValueNull.INSTANCE : ValueString.get(s);
            Column col = columns[i];
            v = col.convert(v);
            values[i] = v;
        }
        Row row = new Row(values, 1);
        row.setKey(rows.size());
        rows.add(row);
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("META");
    }


    @Override
    public String getTableType() {
        return Table.SYSTEM_TABLE;
    }
    

    @Override
    public ArrayList<Index> getIndexes() {
        ArrayList<Index> list = New.arrayList(1); 
        list.add(new Index(this, null, IndexColumn.wrap(columns), IndexType.createScan())); 
        return list;
    }

    @Override
    public Index getUniqueIndex() {
        return null;
    }

    /**
     * Get the number of meta table types. Supported meta table
     * types are 0 .. this value - 1.
     *
     * @return the number of meta table types
     */
    public static int getMetaTableTypeCount() {
        return META_TABLE_TYPE_COUNT;
    }

    @Override
    public long getRowCountApproximation() {
        return ROW_COUNT_APPROXIMATION;
    }


    @Override
    public boolean isDeterministic() {
        return true;
    }

}
