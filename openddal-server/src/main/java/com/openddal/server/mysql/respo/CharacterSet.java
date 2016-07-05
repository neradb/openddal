package com.openddal.server.mysql.respo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.openddal.server.mysql.MySQLProtocolProcessor;
import com.openddal.server.mysql.parser.ServerParseSet;
import com.openddal.server.util.ErrorCode;
import com.openddal.server.util.StringUtil;

/**
 * 字符集属性设置
 */
public class CharacterSet {

    private static final Logger logger = LoggerFactory.getLogger(CharacterSet.class);

    public static void response(String stmt, MySQLProtocolProcessor c, int rs) {
        if (-1 == stmt.indexOf(',')) {
            /* 单个属性 */
            oneSetResponse(stmt, c, rs);
        } else {
            /* 多个属性 ,但是只关注CHARACTER_SET_RESULTS，CHARACTER_SET_CONNECTION */
            multiSetResponse(stmt, c, rs);
        }
    }

    private static void oneSetResponse(String stmt, MySQLProtocolProcessor c, int rs) {
        if ((rs & 0xff) == ServerParseSet.CHARACTER_SET_CLIENT) {
            /* 忽略client属性设置 */
            c.sendOk();
        } else {
            String charset = stmt.substring(rs >>> 8).trim();
            if (charset.endsWith(";")) {
                /* 结尾为 ; 标识符 */
                charset = charset.substring(0, charset.length() - 1);
            }

            if (charset.startsWith("'") || charset.startsWith("`")) {
                /* 与mysql保持一致，引号里的字符集不做trim操作 */
                charset = charset.substring(1, charset.length() - 1);
            }

            // 设置字符集
            setCharset(charset, c);
        }
    }

    private static void multiSetResponse(String stmt, MySQLProtocolProcessor c, int rs) {
        String charResult = "null";
        String charConnection = "null";
        String[] sqlList = StringUtil.split(stmt, ',', false);

        // check first
        switch (rs & 0xff) {
            case ServerParseSet.CHARACTER_SET_RESULTS:
                charResult = sqlList[0].substring(rs >>> 8).trim();
                break;
            case ServerParseSet.CHARACTER_SET_CONNECTION:
                charConnection = sqlList[0].substring(rs >>> 8).trim();
                break;
        }

        // check remaining
        for (int i = 1; i < sqlList.length; i++) {
            String sql = new StringBuilder("set ").append(sqlList[i]).toString();
            if ((i + 1 == sqlList.length) && sql.endsWith(";")) {
                /* 去掉末尾的 ‘;’ */
                sql = sql.substring(0, sql.length() - 1);
            }
            int rs2 = ServerParseSet.parse(sql, "set".length());
            switch (rs2 & 0xff) {
                case ServerParseSet.CHARACTER_SET_RESULTS:
                    charResult = sql.substring(rs2 >>> 8).trim();
                    break;
                case ServerParseSet.CHARACTER_SET_CONNECTION:
                    charConnection = sql.substring(rs2 >>> 8).trim();
                    break;
                case ServerParseSet.CHARACTER_SET_CLIENT:
                    break;
                default:
                    StringBuilder s = new StringBuilder();
                    logger.warn(s.append(sql).append(" is not executed").toString());
            }
        }

        if (charResult.startsWith("'") || charResult.startsWith("`")) {
            charResult = charResult.substring(1, charResult.length() - 1);
        }
        if (charConnection.startsWith("'") || charConnection.startsWith("`")) {
            charConnection = charConnection.substring(1, charConnection.length() - 1);
        }

        // 如果其中一个为null，则以另一个为准。
        if ("null".equalsIgnoreCase(charResult)) {
            setCharset(charConnection, c);
            return;
        }
        if ("null".equalsIgnoreCase(charConnection)) {
            setCharset(charResult, c);
            return;
        }
        if (charConnection.equalsIgnoreCase(charResult)) {
            setCharset(charConnection, c);
        } else {
            StringBuilder sb = new StringBuilder();
            sb.append("charset is not consistent:[connection=").append(charConnection);
            sb.append(",results=").append(charResult).append(']');
            c.sendError(ErrorCode.ER_UNKNOWN_CHARACTER_SET, sb.toString());
        }
    }

    private static void setCharset(String charset, MySQLProtocolProcessor c) {
        if ("null".equalsIgnoreCase(charset)) {
            /* 忽略字符集为null的属性设置 */
            c.sendOk();
        } else if (c.getSession().setCharset(charset)) {
            c.sendOk();
        } else {
            try {
                if (c.getSession().setCharsetIndex(Integer.parseInt(charset))) {
                    c.sendOk();
                } else {
                    c.sendError(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset :" + charset);
                }
            } catch (RuntimeException e) {
                c.sendError(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset :" + charset);
            }
        }
    }

}
