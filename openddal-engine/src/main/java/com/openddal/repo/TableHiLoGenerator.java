package com.openddal.repo;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import com.openddal.config.SequenceRule;
import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.schema.Sequence;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.message.ErrorCode;
import com.openddal.route.rule.ObjectNode;
import com.openddal.util.JdbcUtils;
import com.openddal.util.StringUtils;
import com.openddal.value.DataType;
import com.openddal.value.Value;

public class TableHiLoGenerator extends Sequence {


    /**
     * The default {@link #TABLE_PARAM} value
     */
    public static final String DEF_TABLE = "openddal_sequences";

    /**
     * The default {@link #VALUE_COLUMN_PARAM} value
     */
    public static final String DEF_VALUE_COLUMN = "next_val";

    /**
     * The default {@link #SEGMENT_COLUMN_PARAM} value
     */
    public static final String DEF_NAME_COLUMN = "sequence_name";

    /**
     * The default {@link #SEGMENT_LENGTH_PARAM} value
     */
    public static final int DEF_NAMECOLUMNLENGTH_LENGTH = 255;

    /**
     * The default {@link #INITIAL_PARAM} value
     */
    public static final int DEFAULT_INITIAL_VALUE = 1;

    /**
     * The default {@link #INCREMENT_PARAM} value
     */
    public static final int DEFAULT_INCREMENT_SIZE = 1;

    private ObjectNode tableNode;

    private String nameColumnName;
    private String valueColumnName;
    private String nameValue;
    private int nameColumnLength;
    private int initialValue;
    private int incrementSize;

    private String selectQuery;
    private String insertQuery;
    private String updateQuery;

    private long accessCount;
    private DataSource dataSource;
    private HiloOptimizer optimizer;

    public TableHiLoGenerator(Schema schema, String name, SequenceRule config) {
        super(schema, name, 1, 1);
        configure(config.getProperties());
    }

    public final long getTableAccessCount() {
        return accessCount;
    }

    public void configure(Properties params) {
        JdbcRepository repo = (JdbcRepository) database.getRepository();
        String shardName = params.getProperty("shard", repo.getPublicDB());
        String catalog = params.getProperty("catalog");
        String schema = params.getProperty("schema");
        String tableName = params.getProperty("tableName", DEF_TABLE);
        tableName = database.identifier(tableName);
        if (catalog != null) {
            catalog = database.identifier(catalog);
        }
        if (schema != null) {
            schema = database.identifier(schema);
        }
        tableNode = new ObjectNode(shardName, catalog, schema, tableName, null);
        nameValue = StringUtils.toLowerEnglish(getName());
        nameColumnName = params.getProperty("nameColumnName", DEF_NAME_COLUMN);
        valueColumnName = params.getProperty("valueColumnName", DEF_VALUE_COLUMN);
        nameColumnLength = getIntProperty(params, "nameColumnLength", DEF_NAMECOLUMNLENGTH_LENGTH);
        incrementSize = getIntProperty(params, "cacheSize", (int) getCacheSize());
        initialValue = getIntProperty(params, "initialValue", DEFAULT_INITIAL_VALUE);

        this.selectQuery = buildSelectQuery();
        this.updateQuery = buildUpdateQuery();
        this.insertQuery = buildInsertQuery();

        this.dataSource = repo.getDataSourceByShardName(shardName);
        this.optimizer = new HiloOptimizer(incrementSize);
        
        this.createTableIfNotExits();
    }

    static int getIntProperty(Properties params, String key, int def) {
        String s = params.getProperty(key);
        if (s != null) {
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                // ignore
            }
        }
        return def;
    }

    private void createTableIfNotExits() {
        String tableName = tableNode.getQualifiedObjectName();
        String catalog = tableNode.getCatalog();
        String schema = tableNode.getSchema();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            DatabaseMetaData meta = conn.getMetaData();
            rs = meta.getTables(catalog, schema, tableName, null);
            if (!rs.next()) {
                stmt = conn.createStatement();
                stmt.execute(buildCreateQuery());
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        } finally {
            JdbcUtils.closeSilently(rs);
            JdbcUtils.closeSilently(stmt);
            JdbcUtils.closeSilently(conn);
        }
    }

    protected String buildCreateQuery() {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(tableNode.getCompositeObjectName()).append(" ( ").append(nameColumnName)
                .append(" ").append(DataType.getDataType(Value.STRING).name).append('(').append(nameColumnLength)
                .append(") NOT NULL, ").append(valueColumnName).append(" ")
                .append(DataType.getDataType(Value.LONG).name).append(", PRIMARY KEY ( ").append(nameColumnName)
                .append(" ))");
        return sql.toString();
    }

    protected String buildSelectQuery() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(valueColumnName).append(" FROM ").append(tableNode.getCompositeObjectName())
                .append(" WHERE ").append(nameColumnName).append(" = ?");
        return sql.toString();
    }

    protected String buildUpdateQuery() {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ").append(tableNode.getCompositeObjectName()).append(" SET ").append(valueColumnName)
                .append(" = ?").append(" WHERE ").append(valueColumnName).append(" = ? and ").append(nameColumnName)
                .append(" = ?");
        return sql.toString();
    }

    protected String buildInsertQuery() {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableNode.getCompositeObjectName()).append(" (").append(nameColumnName)
                .append(", ").append(valueColumnName).append(") ").append("VALUES (?, ?)");
        return sql.toString();
    }

    @Override
    public synchronized long getNext(Session session) {
        try {
            return optimizer.generate();
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
    }

    @Override
    public synchronized long getCurrentValue() {
        if (optimizer.value < 1) {
            throw DbException.get(ErrorCode.FEATURE_NOT_SUPPORTED_1,
                    "sequence " + nameValue + ".currval is not yet defined in this session");
        }
        return optimizer.value - 1;
    }

    public long queryNextValue() throws SQLException {
        Connection connection = null;
        long value;
        int rows;
        try {
            do {
                connection = dataSource.getConnection();
                PreparedStatement selectPS = connection.prepareStatement(selectQuery);
                try {
                    selectPS.setString(1, nameValue);
                    final ResultSet selectRS = selectPS.executeQuery();
                    if (!selectRS.next()) {
                        value = initialValue;
                        PreparedStatement insertPS = connection.prepareStatement(insertQuery);
                        try {
                            insertPS.setString(1, nameValue);
                            insertPS.setLong(2, value);
                            insertPS.executeUpdate();
                        } finally {
                            insertPS.close();
                        }
                    } else {
                        long rsValue = selectRS.getLong(1);
                        if (selectRS.wasNull()) {
                            throw new SQLException(nameValue + " " + valueColumnName + " is null");
                        }
                        value = rsValue;
                    }
                    selectRS.close();
                } catch (SQLException e) {
                    throw e;
                } finally {
                    selectPS.close();
                }

                final PreparedStatement updatePS = connection.prepareStatement(updateQuery);
                try {
                    long updateValue = value + 1;
                    updatePS.setLong(1, updateValue);
                    updatePS.setLong(2, value);
                    updatePS.setString(3, nameValue);
                    rows = updatePS.executeUpdate();
                } catch (SQLException e) {
                    throw e;
                } finally {
                    updatePS.close();
                }
            } while (rows == 0);
        } finally {
            JdbcUtils.closeSilently(connection);
        }
        accessCount++;

        return value;

    }

    /**
     * @see https://vladmihalcea.com/2014/06/23/the-hilo-algorithm/
     */
    private class HiloOptimizer {

        private final int incrementSize;
        private long lastSourceValue;
        private long upperLimit;
        private long value;

        public HiloOptimizer(int incrementSize) {
            if (incrementSize < 1) {
                throw new IllegalArgumentException("increment size cannot be less than 1");
            }
            this.incrementSize = incrementSize;
        }

        public long generate() throws SQLException {
            if (lastSourceValue < 1) {
                // first call, we need to read the database
                // value and set up the 'bucket' boundaries
                lastSourceValue = queryNextValue();
                while (lastSourceValue < 1) {
                    lastSourceValue = queryNextValue();
                }
                // upperLimit defines the upper end of the bucket values
                upperLimit = lastSourceValue * incrementSize + 1;
                // initialize value to the low end of the bucket
                value = upperLimit - incrementSize;
            } else if (value >= upperLimit) {
                lastSourceValue = queryNextValue();
                upperLimit = lastSourceValue * incrementSize + 1;
            }
            return value++;
        }
    }
}
