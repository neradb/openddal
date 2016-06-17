package com.openddal.repo;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import com.openddal.dbobject.schema.Schema;
import com.openddal.dbobject.schema.Sequence;
import com.openddal.dbobject.table.Table;

public class TableHiLoSequence extends Sequence {

    public TableHiLoSequence(Schema schema, int id, String name, long startValue, long increment) {
        super(schema, id, name, startValue, increment);
        // TODO Auto-generated constructor stub
    }

    public static final String ID_TABLE = "table";
    public static final String PK_COLUMN_NAME = "primary_key_column";
    public static final String PK_VALUE_NAME = "primary_key_value";
    public static final String VALUE_COLUMN_NAME = "value_column";
    public static final String PK_LENGTH_NAME = "primary_key_length";

    private static final int DEFAULT_PK_LENGTH = 255;
    public static final String DEFAULT_TABLE = "hibernate_sequences";
    private static final String DEFAULT_PK_COLUMN = "sequence_name";
    private static final String DEFAULT_VALUE_COLUMN = "sequence_next_hi_value";

    private String tableName;
    private String pkColumnName;
    private String valueColumnName;
    private String query;
    private String insert;
    private String update;

    // hilo params
    public static final String MAX_LO = "max_lo";

    private int maxLo;
    private LegacyHiLoAlgorithmOptimizer hiloOptimizer;
    private int keySize;

    public String sqlCreateStrings(Dialect dialect) {
        return new StringBuilder(dialect.getCreateTableString()).append(' ').append(tableName).append(" ( ")
                .append(pkColumnName).append(' ').append(dialect.getTypeName(Types.VARCHAR, keySize, 0, 0))
                .append(",  ").append(valueColumnName).append(' ').append(dialect.getTypeName(Types.INTEGER))
                .append(" )").append(dialect.getTableTypeString()).toString();

    }

    public String[] sqlDropStrings(Dialect dialect) {
        return new String[] { dialect.getDropTableString(tableName) };
    }

    public Object generatorKey() {
        return tableName;
    }

    @Override
    public IntegralDataTypeHolder execute(Connection connection) throws SQLException {
        IntegralDataTypeHolder value = IdentifierGeneratorHelper.getIntegralDataTypeHolder(returnClass);

        int rows;
        do {
            final PreparedStatement queryPreparedStatement = prepareStatement(connection, query, statementLogger,
                    statsCollector);
            try {
                final ResultSet rs = executeQuery(queryPreparedStatement, statsCollector);
                boolean isInitialized = rs.next();
                if (!isInitialized) {
                    value.initialize(0);
                    final PreparedStatement insertPreparedStatement = prepareStatement(connection, insert,
                            statementLogger, statsCollector);
                    try {
                        value.bind(insertPreparedStatement, 1);
                        executeUpdate(insertPreparedStatement, statsCollector);
                    } finally {
                        insertPreparedStatement.close();
                    }
                } else {
                    value.initialize(rs, 0);
                }
                rs.close();
            } catch (SQLException sqle) {
                throw sqle;
            } finally {
                queryPreparedStatement.close();
            }

            final PreparedStatement updatePreparedStatement = prepareStatement(connection, update, statementLogger,
                    statsCollector);
            try {
                value.copy().increment().bind(updatePreparedStatement, 1);
                value.bind(updatePreparedStatement, 2);

                rows = executeUpdate(updatePreparedStatement, statsCollector);
            } catch (SQLException sqle) {
                LOG.error(LOG.unableToUpdateHiValue(tableName), sqle);
                throw sqle;
            } finally {
                updatePreparedStatement.close();
            }
        } while (rows == 0);

        return value;
    }

    public synchronized Serializable generate(final SessionImplementor session, Object obj) {
        final SqlStatementLogger statementLogger = session.getFactory().getServiceRegistry()
                .getService(JdbcServices.class).getSqlStatementLogger();
        final SessionEventListenerManager statsCollector = session.getEventListenerManager();

        final WorkExecutorVisitable<IntegralDataTypeHolder> work = new AbstractReturningWork<IntegralDataTypeHolder>() {

        };

        // maxLo < 1 indicates a hilo generator with no hilo :?
        if (maxLo < 1) {
            // keep the behavior consistent even for boundary usages
            IntegralDataTypeHolder value = null;
            while (value == null || value.lt(1)) {
                value = session.getTransactionCoordinator().getTransaction().createIsolationDelegate()
                        .delegateWork(work, true);
            }
            return value.makeValue();
        }

        return hiloOptimizer.generate(new AccessCallback() {
            public IntegralDataTypeHolder getNextValue() {
                return session.getTransactionCoordinator().getTransaction().createIsolationDelegate().delegateWork(work,
                        true);
            }

            @Override
            public String getTenantIdentifier() {
                return session.getTenantIdentifier();
            }
        });
    }

    private PreparedStatement prepareStatement(Connection connection, String sql) throws SQLException {
        try {
            return connection.prepareStatement(sql);
        } finally {
        }
    }

    private int executeUpdate(PreparedStatement ps) throws SQLException {
        try {
            statsCollector.jdbcExecuteStatementStart();
            return ps.executeUpdate();
        } finally {
            statsCollector.jdbcExecuteStatementEnd();
        }

    }

    private ResultSet executeQuery(PreparedStatement ps, SessionEventListenerManager statsCollector)
            throws SQLException {
        try {
            statsCollector.jdbcExecuteStatementStart();
            return ps.executeQuery();
        } finally {
            statsCollector.jdbcExecuteStatementEnd();
        }
    }

    public void configure(Type type, Properties params, Dialect dialect) throws MappingException {
        ObjectNameNormalizer normalizer = (ObjectNameNormalizer) params.get(IDENTIFIER_NORMALIZER);

        tableName = normalizer
                .normalizeIdentifierQuoting(ConfigurationHelper.getString(ID_TABLE, params, DEFAULT_TABLE));
        if (tableName.indexOf('.') < 0) {
            tableName = dialect.quote(tableName);
            final String schemaName = dialect.quote(normalizer.normalizeIdentifierQuoting(params.getProperty(SCHEMA)));
            final String catalogName = dialect
                    .quote(normalizer.normalizeIdentifierQuoting(params.getProperty(CATALOG)));
            tableName = Table.qualify(catalogName, schemaName, tableName);
        } else {
            // if already qualified there is not much we can do in a portable
            // manner so we pass it
            // through and assume the user has set up the name correctly.
        }

        pkColumnName = dialect.quote(normalizer
                .normalizeIdentifierQuoting(ConfigurationHelper.getString(PK_COLUMN_NAME, params, DEFAULT_PK_COLUMN)));
        valueColumnName = dialect.quote(normalizer.normalizeIdentifierQuoting(
                ConfigurationHelper.getString(VALUE_COLUMN_NAME, params, DEFAULT_VALUE_COLUMN)));
        keySize = ConfigurationHelper.getInt(PK_LENGTH_NAME, params, DEFAULT_PK_LENGTH);
        String keyValue = ConfigurationHelper.getString(PK_VALUE_NAME, params, params.getProperty(TABLE));

        query = "select " + valueColumnName + " from " + dialect.appendLockHint(LockMode.PESSIMISTIC_WRITE, tableName)
                + " where " + pkColumnName + " = '" + keyValue + "'" + dialect.getForUpdateString();

        update = "update " + tableName + " set " + valueColumnName + " = ? where " + valueColumnName + " = ? and "
                + pkColumnName + " = '" + keyValue + "'";

        insert = "insert into " + tableName + "(" + pkColumnName + ", " + valueColumnName + ") " + "values('" + keyValue
                + "', ?)";

        // hilo config
        maxLo = ConfigurationHelper.getInt(MAX_LO, params, Short.MAX_VALUE);
        returnClass = type.getReturnedClass();

        if (maxLo >= 1) {
            hiloOptimizer = new LegacyHiLoAlgorithmOptimizer(returnClass, maxLo);
        }
    }

}
