package com.openddal.server.mysql.pcs;

import static com.alibaba.druid.sql.visitor.SQLEvalVisitor.EVAL_VALUE;

import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowAuthorsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowEnginesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowFunctionStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowIndexesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcedureStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowProcessListStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTableStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowTriggersStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowWarningsStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlEvalVisitorImpl;
import com.alibaba.druid.sql.visitor.SQLEvalVisitor;
import com.alibaba.druid.sql.visitor.SQLEvalVisitorUtils;
import com.alibaba.druid.util.JdbcConstants;
import com.openddal.engine.Constants;
import com.openddal.result.LocalResult;
import com.openddal.result.SimpleResultSet;
import com.openddal.server.ServerException;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;
import com.openddal.server.core.ServerSession;
import com.openddal.server.util.ErrorCode;
import com.openddal.util.New;

/**
 * @author jorgie.li
 */
public final class ShowProcessor implements QueryProcessor {

    private DefaultQueryProcessor target;

    public ShowProcessor(DefaultQueryProcessor target) {
        this.target = target;
    }

    @Override
    public QueryResult process(String query) {
        List<SQLStatement> stmts = SQLUtils.parseStatements(query, JdbcConstants.MYSQL);
        for (SQLStatement stmt : stmts) {
            if (stmt instanceof MySqlShowDatabasesStatement) {
                return showDatabase((MySqlShowDatabasesStatement) stmt);
            } else if (stmt instanceof MySqlShowVariantsStatement) {
                return showVariables((MySqlShowVariantsStatement) stmt);
            } else if (stmt instanceof SQLShowTablesStatement) {
                return showTables((SQLShowTablesStatement) stmt);
            } else if (stmt instanceof MySqlShowAuthorsStatement) {
                return showAuthors((MySqlShowAuthorsStatement) stmt);
            } else if (stmt instanceof MySqlShowCharacterSetStatement) {
                return showCharacterSet((MySqlShowCharacterSetStatement) stmt);
            } else if (stmt instanceof MySqlShowCollationStatement) {
                return showCollation((MySqlShowCollationStatement) stmt);
            } else if (stmt instanceof MySqlShowColumnsStatement) {
                return showColumns((MySqlShowColumnsStatement) stmt);
            } else if (stmt instanceof MySqlShowStatusStatement) {
                return showStatus((MySqlShowStatusStatement) stmt);
            } else if (stmt instanceof MySqlShowSlaveStatusStatement) {
                return showSlaveStatus((MySqlShowSlaveStatusStatement) stmt);
            } else if (stmt instanceof MySqlShowEnginesStatement) {
                return showEngines((MySqlShowEnginesStatement) stmt);
            } else if (stmt instanceof MySqlShowWarningsStatement) {
                return showWarnings((MySqlShowWarningsStatement) stmt);
            } else if (stmt instanceof MySqlShowProcessListStatement) {
                return showProcessList((MySqlShowProcessListStatement) stmt);
            } else if (stmt instanceof MySqlShowProcedureStatusStatement) {
                return showProcedureStatus((MySqlShowProcedureStatusStatement) stmt);
            } else if (stmt instanceof MySqlShowFunctionStatusStatement) {
                return showFunctionStatus((MySqlShowFunctionStatusStatement) stmt);
            } else if (stmt instanceof MySqlShowIndexesStatement) {
                return showIndexes((MySqlShowIndexesStatement) stmt);
            } else if (stmt instanceof MySqlShowTriggersStatement) {
                return showTriggers((MySqlShowTriggersStatement) stmt);
            } else if (stmt instanceof MySqlShowCreateTableStatement) {
                return showCreateTable((MySqlShowCreateTableStatement) stmt);
            } else if (stmt instanceof MySqlShowTableStatusStatement) {
                throw ServerException.get(ErrorCode.ER_NOT_ALLOWED_COMMAND, "not allowed command:" + query);
            }
        }
        return target.process(query);
    }

    private QueryResult showCreateTable(MySqlShowCreateTableStatement stmt) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Table", Types.VARCHAR, 0, 0);
        result.addColumn("Create Table", Types.VARCHAR, 0, 0);
        result.addRow(new Object[]{SQLUtils.toMySqlString(stmt.getName())},"");
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
        
    }

    private QueryResult showTriggers(MySqlShowTriggersStatement stmt) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Trigger", Types.VARCHAR, 0, 0);
        result.addColumn("Event", Types.VARCHAR, 0, 0);
        result.addColumn("Table", Types.VARCHAR, 0, 0);
        result.addColumn("Statement", Types.VARCHAR, 0, 0);
        result.addColumn("Timing", Types.VARCHAR, 0, 0);
        result.addColumn("Created", Types.VARCHAR, 0, 0);
        result.addColumn("sql_mode", Types.VARCHAR, 0, 0);
        result.addColumn("Definer", Types.VARCHAR, 0, 0);
        result.addColumn("character_set_client", Types.VARCHAR, 0, 0);
        result.addColumn("collation_connection", Types.VARCHAR, 0, 0);
        result.addColumn("Database Collation", Types.VARCHAR, 0, 0);
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showIndexes(MySqlShowIndexesStatement stmt) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Table", Types.VARCHAR, 0, 0);
        result.addColumn("Non_unique", Types.VARCHAR, 0, 0);
        result.addColumn("Key_name", Types.VARCHAR, 0, 0);
        result.addColumn("Seq_in_index", Types.VARCHAR, 0, 0);
        result.addColumn("Collation", Types.VARCHAR, 0, 0);
        result.addColumn("Cardinality", Types.VARCHAR, 0, 0);
        result.addColumn("Sub_part", Types.VARCHAR, 0, 0);
        result.addColumn("Packed", Types.VARCHAR, 0, 0);
        result.addColumn("Null", Types.VARCHAR, 0, 0);
        result.addColumn("Index_type", Types.VARCHAR, 0, 0);
        result.addColumn("Comment", Types.VARCHAR, 0, 0);
        result.addColumn("Index_comment", Types.VARCHAR, 0, 0);
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showFunctionStatus(MySqlShowFunctionStatusStatement stmt) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Db", Types.VARCHAR, 0, 0);
        result.addColumn("Name", Types.VARCHAR, 0, 0);
        result.addColumn("Type", Types.VARCHAR, 0, 0);
        result.addColumn("Definer", Types.VARCHAR, 0, 0);
        result.addColumn("Created", Types.VARCHAR, 0, 0);
        result.addColumn("Security_type", Types.VARCHAR, 0, 0);
        result.addColumn("Comment", Types.VARCHAR, 0, 0);
        result.addColumn("character_set_client", Types.VARCHAR, 0, 0);
        result.addColumn("collation_connection", Types.VARCHAR, 0, 0);
        result.addColumn("Database Collation", Types.VARCHAR, 0, 0);
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showProcedureStatus(MySqlShowProcedureStatusStatement stmt) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Db", Types.VARCHAR, 0, 0);
        result.addColumn("Name", Types.VARCHAR, 0, 0);
        result.addColumn("Type", Types.VARCHAR, 0, 0);
        result.addColumn("Definer", Types.VARCHAR, 0, 0);
        result.addColumn("Created", Types.VARCHAR, 0, 0);
        result.addColumn("Security_type", Types.VARCHAR, 0, 0);
        result.addColumn("Comment", Types.VARCHAR, 0, 0);
        result.addColumn("character_set_client", Types.VARCHAR, 0, 0);
        result.addColumn("collation_connection", Types.VARCHAR, 0, 0);
        result.addColumn("Database Collation", Types.VARCHAR, 0, 0);
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showWarnings(MySqlShowWarningsStatement stmt) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Level", Types.VARCHAR, 0, 0);
        result.addColumn("Code", Types.VARCHAR, 0, 0);
        result.addColumn("Message", Types.VARCHAR, 0, 0);
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showProcessList(MySqlShowProcessListStatement stmt) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Id", Types.VARCHAR, 0, 0);
        result.addColumn("User", Types.VARCHAR, 0, 0);
        result.addColumn("Host", Types.VARCHAR, 0, 0);
        result.addColumn("db", Types.VARCHAR, 0, 0);
        result.addColumn("Command", Types.VARCHAR, 0, 0);
        result.addColumn("Time", Types.VARCHAR, 0, 0);
        result.addColumn("State", Types.VARCHAR, 0, 0);
        result.addColumn("Info", Types.VARCHAR, 0, 0);

        Collection<ServerSession> sessions = target.getSession().getServer().getSessions();
        long time = System.currentTimeMillis();
        for (ServerSession s : sessions) {
            result.addRow(new Object[] { s.getThreadId(), s.getUser(), s.getAttachment("remoteAddress"), s.getSchema(),
                    "Query", (time - s.getUptime()), "", "" });
        }

        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showEngines(MySqlShowEnginesStatement stmt) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Engine", Types.VARCHAR, 0, 0);
        result.addColumn("Support", Types.VARCHAR, 0, 0);
        result.addColumn("Comment", Types.VARCHAR, 0, 0);
        result.addColumn("Transactions", Types.VARCHAR, 0, 0);
        result.addColumn("XA", Types.VARCHAR, 0, 0);
        result.addColumn("Savepoints", Types.VARCHAR, 0, 0);

        result.addRow(new Object[] { "OpenDDAL", "YES", "Distributed sql engine", "YES", "NO", "YES" });
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));

    }

    private QueryResult showVariables(MySqlShowVariantsStatement s) {
        Map<String, String> variables = target.getSession().getServer().getVariables();
        SQLExpr where = s.getWhere();
        SQLCharExpr like = (SQLCharExpr) s.getLike();
        return filter(variables, where, like);
    }

    private QueryResult showStatus(MySqlShowStatusStatement s) {
        Map<String, String> status = target.getSession().getServer().getStatus();
        SQLExpr where = s.getWhere();
        SQLCharExpr like = (SQLCharExpr) s.getLike();
        return filter(status, where, like);
    }

    private QueryResult filter(Map<String, String> variables, SQLExpr where, SQLCharExpr like) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Variable_name", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Value", Types.VARCHAR, Integer.MAX_VALUE, 0);
        if (like != null) {
            for (Map.Entry<String, String> item : variables.entrySet()) {
                String key = item.getKey();
                String value = item.getValue();
                if (SQLEvalVisitorUtils.like(key, like.getText())) {
                    result.addRow(new Object[] { key, value });
                }
            }
        } else if (where != null) {
            for (Map.Entry<String, String> item : variables.entrySet()) {
                final String key = item.getKey();
                final String value = item.getValue();
                SQLEvalVisitor visitor = new MySqlEvalVisitorImpl() {
                    public boolean visit(SQLIdentifierExpr x) {
                        String name = x.getName();
                        if ("Variable_name".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, key);
                        } else if ("Value".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, value);
                        }
                        return false;
                    }
                };
                where.accept(visitor);
                Object b = where.getAttributes().get(EVAL_VALUE);
                boolean added = b instanceof Boolean ? (Boolean) b : false;
                if (added) {
                    result.addRow(new Object[] { key, value });
                }

            }
        } else {
            for (Map.Entry<String, String> item : variables.entrySet()) {
                result.addRow(new Object[] { item.getKey(), item.getValue() });
            }
        }
        // we have to convert result.
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showDatabase(MySqlShowDatabasesStatement s) {
        StringBuilder sql = new StringBuilder("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ");
        SQLExpr like = s.getLike();
        SQLExpr where = s.getWhere();
        if (like != null) {
            sql.append(" WHERE SCHEMA_NAME").append("LIKE ").append(SQLUtils.toMySqlString(like));
        } else if (where != null) {
            sql.append(" WHERE ").append(SQLUtils.toMySqlString(where));
        }
        return target.process(sql.toString());
    }

    private QueryResult showTables(SQLShowTablesStatement s) {
        StringBuilder sql = new StringBuilder(
                "SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ");
        String schema = Constants.SCHEMA_MAIN;
        SQLName database = s.getDatabase();
        SQLExpr like = s.getLike();
        SQLExpr where = s.getWhere();
        if (database != null) {
            schema = SQLUtils.toMySqlString(database);
            schema = schema.replaceAll("['|`]", "");
        }
        sql.append("'").append(schema).append("'");

        if (like != null) {
            sql.append(" AND TABLE_NAME").append("LIKE ").append(SQLUtils.toMySqlString(database));
        } else if (where != null) {
            sql.append(" AND ").append(SQLUtils.toMySqlString(where));
        }
        sql.append(" ORDER BY TABLE_NAME");
        return target.process(sql.toString());
    }

    private QueryResult showColumns(MySqlShowColumnsStatement s) {
        // not support like and where
        s.setLike(null);
        s.setWhere(null);
        s.setFull(false);
        return target.process(SQLUtils.toMySqlString(s));
    }

    private QueryResult showAuthors(MySqlShowAuthorsStatement s) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Name", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Location", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Comment", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addRow(new Object[] { "jorgie.li", "GuangZhou, China", "Architecture, coding and archive" });
        // we have to convert result.
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showSlaveStatus(MySqlShowSlaveStatusStatement stmt) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Slave_IO_State", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_Host", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_User", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_Port", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Connect_Retry", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_Log_File", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Read_Master_Log_Pos", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Relay_Log_File", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Relay_Log_Pos", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Relay_Master_Log_File", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Slave_IO_Running", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Slave_SQL_Running", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Replicate_Do_DB", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Replicate_Ignore_DB", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Replicate_Do_Table", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Replicate_Ignore_Table", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Replicate_Wild_Do_Table", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Replicate_Wild_Ignore_Table", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Last_Errno", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Last_Error", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Skip_Counter", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Exec_Master_Log_Pos", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Relay_Log_Space", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Until_Condition", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Until_Log_File", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Until_Log_Pos", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_SSL_Allowed", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_SSL_CA_File", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_SSL_CA_Path", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_SSL_Cert", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_SSL_Cipher", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_SSL_Key", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Seconds_Behind_Master", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_SSL_Verify_Server_Cert", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Last_IO_Errno", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Last_IO_Error", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Replicate_Ignore_Server_Ids", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_Server_Id", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_UUID", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_Info_File", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("SQL_Delay", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("SQL_Remaining_Delay", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Slave_SQL_Running_State", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_Retry_Count", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_Bind", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Last_IO_Error_Timestamp", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Last_SQL_Error_Timestamp", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_SSL_Crl", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_SSL_Crlpath", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Retrieved_Gtid_Set", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Executed_Gtid_Set", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Auto_Position", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Replicate_Rewrite_DB", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Channel_Name", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Master_TLS_Version", Types.VARCHAR, Integer.MAX_VALUE, 0);
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showCharacterSet(MySqlShowCharacterSetStatement s) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Charset", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Description", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Default collation", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Maxlen", Types.VARCHAR, Integer.MAX_VALUE, 0);

        List<Object[]> charsets = New.arrayList(6);
        charsets.add(new Object[] { "latin1", "cp1252 West European", "latin1_swedish_ci", "1" });
        charsets.add(new Object[] { "gb2312", "GB2312 Simplified Chinese", "gb2312_chinese_ci", "2" });
        charsets.add(new Object[] { "gbk", "GBK Simplified Chinese", "gbk_chinese_ci", "2" });
        charsets.add(new Object[] { "utf8", "UTF-8 Unicode", "utf8_general_ci", "3" });
        charsets.add(new Object[] { "utf8mb4", "UTF-8 Unicode", "utf8mb4_general_ci", "4" });
        charsets.add(new Object[] { "utf16", "UTF-16 Unicode", "utf16_general_ci", "4" });
        SQLCharExpr like = (SQLCharExpr) s.getPattern();
        SQLExpr where = s.getWhere();
        if (like != null) {
            for (Iterator<Object[]> iterator = charsets.iterator(); iterator.hasNext();) {
                Object[] row = iterator.next();
                String charset = row[0].toString();
                if (!SQLEvalVisitorUtils.like(charset, like.getText())) {
                    iterator.remove();
                }
            }
        } else if (where != null) {
            for (Iterator<Object[]> iterator = charsets.iterator(); iterator.hasNext();) {
                final Object[] row = iterator.next();
                SQLEvalVisitor visitor = new MySqlEvalVisitorImpl() {
                    public boolean visit(SQLIdentifierExpr x) {
                        String name = x.getName();
                        if ("Charset".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, row[0].toString());
                        } else if ("Description".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, row[1].toString());
                        } else if ("Default collation".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, row[2].toString());
                        } else if ("Maxlen".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, row[3].toString());
                        }
                        return false;
                    }
                };
                where.accept(visitor);
                Object b = where.getAttributes().get(EVAL_VALUE);
                boolean added = b instanceof Boolean ? (Boolean) b : false;
                if (!added) {
                    iterator.remove();
                }
            }

        }
        for (Object[] objects : charsets) {
            result.addRow(objects);
        }
        // we have to convert result.
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));
    }

    private QueryResult showCollation(MySqlShowCollationStatement s) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("Collation", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Charset", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Id", Types.INTEGER, Integer.MAX_VALUE, 0);
        result.addColumn("Default", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Compiled", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("Sortlen", Types.INTEGER, Integer.MAX_VALUE, 0);

        List<Object[]> charsets = New.arrayList(6);
        charsets.add(new Object[] { "latin1_swedish_ci", "latin1", "8", "Yes", "Yes", "1" });
        charsets.add(new Object[] { "gbk_chinese_ci", "gbk", "28", "Yes", "Yes", "1" });
        charsets.add(new Object[] { "utf8_general_ci", "utf8", "33", "Yes", "Yes", "1" });
        charsets.add(new Object[] { "utf8mb4_general_ci", "utf8mb4", "45", "Yes", "Yes", "1" });
        charsets.add(new Object[] { "utf8mb4", "UTF-8 Unicode", "utf8mb4_general_ci", "4" });
        charsets.add(new Object[] { "utf16_general_ci", "utf16", "54", "Yes", "Yes", "1" });
        SQLCharExpr like = (SQLCharExpr) s.getPattern();
        SQLExpr where = s.getWhere();
        if (like != null) {
            for (Iterator<Object[]> iterator = charsets.iterator(); iterator.hasNext();) {
                Object[] row = iterator.next();
                String collation = row[0].toString();
                if (!SQLEvalVisitorUtils.like(collation, like.getText())) {
                    iterator.remove();
                }
            }
        } else if (where != null) {
            for (Iterator<Object[]> iterator = charsets.iterator(); iterator.hasNext();) {
                final Object[] row = iterator.next();
                SQLEvalVisitor visitor = new MySqlEvalVisitorImpl() {
                    public boolean visit(SQLIdentifierExpr x) {
                        String name = x.getName();
                        if ("Collation".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, row[0].toString());
                        } else if ("Charset".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, row[1].toString());
                        } else if ("Id".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, row[2].toString());
                        } else if ("Default".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, row[3].toString());
                        } else if ("Compiled".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, row[4].toString());
                        } else if ("Sortlen".equalsIgnoreCase(name)) {
                            x.getAttributes().put(EVAL_VALUE, row[5].toString());
                        }
                        return false;
                    }
                };
                where.accept(visitor);
                Object b = where.getAttributes().get(EVAL_VALUE);
                boolean added = b instanceof Boolean ? (Boolean) b : false;
                if (!added) {
                    iterator.remove();
                }
            }

        }
        for (Object[] objects : charsets) {
            result.addRow(objects);
        }
        // we have to convert result.
        return new QueryResult(LocalResult.read(target.getSession().getDbSession(), result, 0));

    }

}
