package com.openddal.server.mysql.respo;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.Map;

import com.openddal.result.SimpleResultSet;
import com.openddal.server.mysql.MySQLServer;
import com.openddal.util.New;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public final class ShowVariables {

    private static final Map<String, String> variables = New.hashMap();

    static {
        variables.put("character_set_client", "utf8");
        variables.put("character_set_connection", "utf8");
        variables.put("character_set_results", "utf8");
        variables.put("character_set_server", "utf8");
        variables.put("init_connect", "");
        variables.put("interactive_timeout", "172800");
        variables.put("lower_case_table_names", "1");
        variables.put("max_allowed_packet", "16777216");
        variables.put("net_buffer_length", "8192");
        variables.put("net_write_timeout", "60");
        variables.put("query_cache_size", "0");
        variables.put("query_cache_type", "OFF");
        variables.put("sql_mode", "STRICT_TRANS_TABLES");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("lower_case_table_names", "1");
        variables.put("tx_isolation", "REPEATABLE-READ");
        variables.put("wait_timeout", "172800");
    }

    public static ResultSet getResultSet() {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("VARIABLE_NAME", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("VALUE", Types.VARCHAR, Integer.MAX_VALUE, 0);
        for (Map.Entry<String, String> foreach : variables.entrySet()) {
            result.addRow(foreach.getKey(), foreach.getValue());
        }
        return result;
    }

    public static ResultSet getShowResultSet(String sql) {
        SimpleResultSet result = new SimpleResultSet();
        result.addColumn("VARIABLE_NAME", Types.VARCHAR, Integer.MAX_VALUE, 0);
        result.addColumn("VALUE", Types.VARCHAR, Integer.MAX_VALUE, 0);
        if ("SHOW SESSION VARIABLES LIKE 'lower_case_table_names'".equalsIgnoreCase(sql.trim())) {
            result.addRow("lower_case_table_names", variables.get("lower_case_table_names"));

        } else if ("SHOW SESSION VARIABLES LIKE 'sql_mode'".equalsIgnoreCase(sql.trim())) {
            result.addRow("sql_mode", variables.get("sql_mode"));

        } else if ("SHOW SESSION VARIABLES LIKE 'version_comment'".equalsIgnoreCase(sql.trim())) {
            result.addRow("Ssl_cipher", MySQLServer.VERSION_COMMENT);

        } else if ("SHOW SESSION VARIABLES LIKE 'version'".equalsIgnoreCase(sql.trim())) {
            result.addRow("version", MySQLServer.SERVER_VERSION);

        } else if ("SHOW SESSION VARIABLES LIKE 'version_compile_os'".equalsIgnoreCase(sql.trim())) {
            result.addRow("version_compile_os", System.getProperty("os"));

        } else if ("SHOW SESSION STATUS LIKE 'Ssl_cipher'".equalsIgnoreCase(sql.trim())) {
            result.addRow("Ssl_cipher", "DHE-RSA-AES256-SHA");

        } else {
            for (Map.Entry<String, String> foreach : variables.entrySet()) {
                result.addRow(foreach.getKey(), foreach.getValue());
            }
        }
        return result;
    }

}