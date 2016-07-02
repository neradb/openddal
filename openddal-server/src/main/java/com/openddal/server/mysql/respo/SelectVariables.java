/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese 
 * opensource volunteers. you can redistribute it and/or modify it under the 
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 * 
 * Any questions about this component can be directed to it's project Web address 
 * https://code.google.com/p/opencloudb/.
 *
 */
package com.openddal.server.mysql.respo;

import java.sql.ResultSet;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Splitter;
import com.openddal.result.SimpleResultSet;
import com.openddal.util.New;


public final class SelectVariables {

    public static ResultSet getResultSet(String sql) {

        SimpleResultSet result = new SimpleResultSet();

        String subSql = sql.substring(sql.indexOf("SELECT") + 6);
        List<String> splitVar = Splitter.on(",").omitEmptyStrings().trimResults().splitToList(subSql);
        splitVar = convert(splitVar);
        int conut = splitVar.size();
        for (int i = 0; i < conut; i++) {
            String s = splitVar.get(i);
            result.addColumn(s, Types.VARCHAR, Integer.MAX_VALUE, 0);
        }
        Object[] row = new Object[conut];
        for (int i = 0; i < conut; i++) {
            String s = splitVar.get(i);
            String value = variables.get(s) == null ? "" : variables.get(s);
            row[i] = value;
        }
        result.addRow(row);
        return result;
    }

    private static List<String> convert(List<String> in) {
        List<String> out = New.arrayList(in.size());
        for (String s : in) {
            int asIndex = s.toUpperCase().indexOf(" AS ");
            if (asIndex != -1) {
                out.add(s.substring(asIndex + 4));
            }
        }
        if (out.isEmpty()) {
            return in;
        } else {
            return out;
        }

    }

    private static final Map<String, String> variables = new HashMap<String, String>();

    static {
        variables.put("@@character_set_client", "utf8");
        variables.put("@@character_set_connection", "utf8");
        variables.put("@@character_set_results", "utf8");
        variables.put("@@character_set_server", "utf8");
        variables.put("@@init_connect", "");
        variables.put("@@interactive_timeout", "172800");
        variables.put("@@license", "GPL");
        variables.put("@@lower_case_table_names", "1");
        variables.put("@@max_allowed_packet", "16777216");
        variables.put("@@net_buffer_length", "16384");
        variables.put("@@net_write_timeout", "60");
        variables.put("@@query_cache_size", "0");
        variables.put("@@query_cache_type", "OFF");
        variables.put("@@sql_mode", "STRICT_TRANS_TABLES");
        variables.put("@@system_time_zone", "CST");
        variables.put("@@time_zone", "SYSTEM");
        variables.put("@@tx_isolation", "REPEATABLE-READ");
        variables.put("@@wait_timeout", "172800");
        variables.put("@@session.auto_increment_increment", "1");

        variables.put("character_set_client", "utf8");
        variables.put("character_set_connection", "utf8");
        variables.put("character_set_results", "utf8");
        variables.put("character_set_server", "utf8");
        variables.put("init_connect", "");
        variables.put("interactive_timeout", "172800");
        variables.put("license", "GPL");
        variables.put("lower_case_table_names", "1");
        variables.put("max_allowed_packet", "16777216");
        variables.put("net_buffer_length", "16384");
        variables.put("net_write_timeout", "60");
        variables.put("query_cache_size", "0");
        variables.put("query_cache_type", "OFF");
        variables.put("sql_mode", "STRICT_TRANS_TABLES");
        variables.put("system_time_zone", "CST");
        variables.put("time_zone", "SYSTEM");
        variables.put("tx_isolation", "REPEATABLE-READ");
        variables.put("wait_timeout", "172800");
        variables.put("auto_increment_increment", "1");
    }

}