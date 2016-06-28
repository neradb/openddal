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
package com.openddal.engine;

import java.util.HashMap;

/**
 * This class contains various database-level settings. To override the
 * documented default value for a database, append the setting in the database
 * URL: "jdbc:h2:test;ALIAS_COLUMN_NAME=TRUE" when opening the first connection
 * to the database. The settings can not be changed once the database is open.
 * <p>
 * Some settings are a last resort and temporary solution to work around a
 * problem in the application or database engine. Also, there are system
 * properties to enable features that are not yet fully tested or that are not
 * backward compatible.
 * </p>
 */
public class DbSettings extends SettingsBase {

    private static DbSettings defaultSettings;

    /**
     * Database setting <code>ALIAS_COLUMN_NAME</code> (default: false).<br />
     * When enabled, aliased columns (as in SELECT ID AS I FROM TEST) return the
     * alias (I in this case) in ResultSetMetaData.getColumnName() and 'null' in
     * getTableName(). If disabled, the real column name (ID in this case) and
     * table name is returned.
     * <br />
     * This setting only affects the default and the MySQL mode. When using
     * any other mode, this feature is enabled for compatibility, even if this
     * database setting is not enabled explicitly.
     */
    public final boolean aliasColumnName = get("ALIAS_COLUMN_NAME", false);

    /**
     * Database setting <code>ANALYZE_SAMPLE</code> (default: 10000).<br />
     * The default sample size when analyzing a table.
     */
    public final int analyzeSample = get("ANALYZE_SAMPLE", 10000);

    /**
     * Database setting <code>DATABASE_TO_UPPER</code> (default: true).<br />
     * Database short names are converted to uppercase for the DATABASE()
     * function, and in the CATALOG column of all database meta data methods.
     * Setting this to "false" is experimental. When set to false, all
     * identifier names (table names, column names) are case sensitive (except
     * aggregate, built-in functions, data types, and keywords).
     */
    public final boolean databaseToUpper = get("DATABASE_TO_UPPER", true);
    
    /**
     * Database setting <code>DEFAULT_ESCAPE</code> (default: \).<br />
     * The default escape character for LIKE comparisons. To select no escape
     * character, use an empty string.
     */
    public final String defaultEscape = get("DEFAULT_ESCAPE", "\\"); 
    /**
     * Database setting <code>DROP_RESTRICT</code> (default: true).<br />
     * Whether the default action for DROP TABLE and DROP VIEW is RESTRICT.
     */
    public final boolean dropRestrict = get("DROP_RESTRICT", true);
    /**
     * Database setting <code>EARLY_FILTER</code> (default: false).<br />
     * This setting allows table implementations to apply filter conditions
     * early on.
     */
    public final boolean earlyFilter = get("EARLY_FILTER", true);
    /**
     * Database setting <code>ESTIMATED_FUNCTION_TABLE_ROWS</code> (default:
     * 1000).<br />
     * The estimated number of rows in a function table (for example, CSVREAD or
     * FTL_SEARCH). This value is used by the optimizer.
     */
    public final int estimatedFunctionTableRows = get(
            "ESTIMATED_FUNCTION_TABLE_ROWS", 1000);
    /**
     * Database setting <code>MAX_COMPACT_TIME</code> (default: 200).<br />
     * The maximum time in milliseconds used to compact a database when closing.
     */
    public final int maxCompactTime = get("MAX_COMPACT_TIME", 200);
    /**
     * Database setting <code>NESTED_JOINS</code> (default: true).<br />
     * Whether nested joins should be supported.
     */
    public final boolean nestedJoins = get("NESTED_JOINS", true);
    /**
     * Database setting <code>OPTIMIZE_DISTINCT</code> (default: true).<br />
     * Improve the performance of simple DISTINCT queries if an index is
     * available for the given column. The optimization is used if:
     * <ul>
     * <li>The select is a single column query without condition </li>
     * <li>The query contains only one table, and no group by </li>
     * <li>There is only one table involved </li>
     * <li>There is an ascending index on the column </li>
     * <li>The selectivity of the column is below 20 </li>
     * </ul>
     */
    public final boolean optimizeDistinct = get("OPTIMIZE_DISTINCT", true);
    /**
     * Database setting <code>OPTIMIZE_EVALUATABLE_SUBQUERIES</code> (default:
     * true).<br />
     * Optimize subqueries that are not dependent on the outer query.
     */
    public final boolean optimizeEvaluatableSubqueries = get(
            "OPTIMIZE_EVALUATABLE_SUBQUERIES", true);
    /**
     * Database setting <code>OPTIMIZE_INSERT_FROM_SELECT</code>
     * (default: true).<br />
     * Insert into table from query directly bypassing temporary disk storage.
     * This also applies to create table as select.
     */
    public final boolean optimizeInsertFromSelect = get(
            "OPTIMIZE_INSERT_FROM_SELECT", true);
    /**
     * Database setting <code>OPTIMIZE_IN_LIST</code> (default: true).<br />
     * Optimize IN(...) and IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE.
     */
    public final boolean optimizeInList = get("OPTIMIZE_IN_LIST", true);
    /**
     * Database setting <code>OPTIMIZE_IN_SELECT</code> (default: true).<br />
     * Optimize IN(SELECT ...) comparisons. This includes
     * optimization for SELECT, DELETE, and UPDATE.
     */
    public final boolean optimizeInSelect = get("OPTIMIZE_IN_SELECT", true);
    /**
     * Database setting <code>OPTIMIZE_IS_NULL</code> (default: false).<br />
     * Use an index for condition of the form columnName IS NULL.
     */
    public final boolean optimizeIsNull = get("OPTIMIZE_IS_NULL", true);
    /**
     * Database setting <code>OPTIMIZE_OR</code> (default: true).<br />
     * Convert (C=? OR C=?) to (C IN(?, ?)).
     */
    public final boolean optimizeOr = get("OPTIMIZE_OR", true);
    /**
     * Database setting <code>OPTIMIZE_TWO_EQUALS</code> (default: true).<br />
     * Optimize expressions of the form A=B AND B=1. In this case, AND A=1 is
     * added so an index on A can be used.
     */
    public final boolean optimizeTwoEquals = get("OPTIMIZE_TWO_EQUALS", true);
    /**
     * Database setting <code>OPTIMIZE_UPDATE</code> (default: true).<br />
     * Speed up inserts, updates, and deletes by not reading all rows from a
     * page unless necessary.
     */
    public final boolean optimizeUpdate = get("OPTIMIZE_UPDATE", true);
    /**
     * Database setting <code>QUERY_CACHE_SIZE</code> (default: 8).<br />
     * The size of the query cache, in number of cached statements. Each session
     * has it's own cache with the given size. The cache is only used if the SQL
     * statement and all parameters match. Only the last returned result per
     * query is cached. The following statement types are cached: SELECT
     * statements are cached (excluding UNION and FOR UPDATE statements), CALL
     * if it returns a single value, DELETE, INSERT, MERGE, UPDATE, and
     * transactional statements such as COMMIT. This works for both statements
     * and prepared statement.
     */
    public final int queryCacheSize = get("QUERY_CACHE_SIZE", 8); 
    /**
     * Database setting <code>ROWID</code> (default: true).<br />
     * If set, each table has a pseudo-column _ROWID_.
     */
    public final boolean rowId = get("ROWID", true);
    /**
     * Database setting <code>SQL_MODE</code> (default: REGULAR).<br />
     */
    public final String sqlMode = get("SQL_MODE", Mode.REGULAR);
    /**
     * Database setting <code>TRANSACTION_MODE</code> (default: null).<br />
     */
    public final String transactionMode = get("TRANSACTION_MODE", null);
    /**
     * Database setting <code>MAX_QUERY_TIMEOUT</code> (default: 0).<br />
     * The maximum timeout of a query in milliseconds. The default is 0, meaning
     * no limit. Please note the actual query timeout may be set to a lower
     * value.
     */
    public int defaultQueryTimeout = get("DEFAULT_QUERY_TIMEOUT", 0);
    /**
     * Database setting <code>DEFAULT_TABLE_ENGINE</code>
     * (default: null).<br />
     * The default table engine to use for new tables.
     */
    public String defaultTableEngine = get("DEFAULT_TABLE_ENGINE", null);
    /**
     * Database setting <code>DEFAULT_VALIDATION_QUERY</code>
     * (default: null).<br />
     * The default sql to validation jdbc connection.
     */
    public String validationQuery = get("VALIDATION_QUERY", null);
    /**
     * Database setting <code>DEFAULT_VALIDATION_QUERYT_IMEOUT</code>
     * (default: -1).<br />
     * The default time to execute validation sql.
     */
    public int validationQueryTimeout = get("VALIDATION_QUERY_TIMEOUT", -1);
    /**
     * Database setting <code>OPTIMIZE_MERGING</code> (default: false).<br />
     * Use union select for query multi-table in same shard.
     */
    public final boolean optimizeMerging = get("OPTIMIZE_MERGING", true);
    


    private DbSettings(HashMap<String, String> s) {
        super(s);
    }

    /**
     * INTERNAL.
     * Get the settings for the given properties (may not be null).
     *
     * @param s the settings
     * @return the settings
     */
    public static DbSettings getInstance(HashMap<String, String> s) {
        return new DbSettings(s);
    }

    /**
     * INTERNAL.
     * Get the default settings. Those must not be modified.
     *
     * @return the settings
     */
    public static DbSettings getDefaultSettings() {
        if (defaultSettings == null) {
            defaultSettings = new DbSettings(new HashMap<String, String>());
        }
        return defaultSettings;
    }
}
