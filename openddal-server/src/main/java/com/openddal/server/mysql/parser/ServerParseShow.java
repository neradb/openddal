package com.openddal.server.mysql.parser;

import com.openddal.server.util.ParseUtil;

/**
 * @author xianmao.hexm 2011-5-7 下午01:23:06
 */
public final class ServerParseShow {

    public static final int OTHER         = -1;
    public static final int DATABASES     = 1;
    public static final int DATASOURCES   = 2;
    public static final int COBAR_STATUS  = 3;
    public static final int COBAR_CLUSTER = 4;
    public static final int SLOW          = 5;
    public static final int PHYSICAL_SLOW = 6;
    public static final int CONNECTION    = 7;
    public static final int VARIABLES     = 8;
    public static final int SESSION_VARIABLES = 9;
    public static final int SESSION_STATUS = 10;
    // show engines
    // @author little-pan
    // @since 2016-07-13
    public static final int ENGINES        = 11;

    public static int parse(String stmt, int offset) {
        int i = offset;
        for (; i < stmt.length(); i++) {
            switch (stmt.charAt(i)) {
            case ' ':
                continue;
            case '/':
            case '#':
                i = ParseUtil.comment(stmt, i);
                continue;
            case 'C':
            case 'c':
                return connectionCheck(stmt, i);
            case 'V':
            case 'v':
                return showVCheck(stmt, i);
            case 'D':
            case 'd':
                return dataCheck(stmt, i);
            case 'E':
            case 'e':
            	return engineCheck(stmt, i);
            case 'G':
            case 'g':
                return showGVCheck(stmt, i);
            case 'S':
            case 's':
                int slowCheck = slowCheck(stmt, i);
                return slowCheck == OTHER ? showSVCheck(stmt, i) : slowCheck;
            case 'p':
            case 'P':
                return physicalCheck(stmt, i);
            default:
                return OTHER;
            }
        }
        return OTHER;
    }

    static int connectionCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ONNECTION".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'N' || c2 == 'n') && (c3 == 'N' || c3 == 'n')
                && (c4 == 'E' || c4 == 'e') && (c5 == 'C' || c5 == 'c') && (c6 == 'T' || c6 == 't')
                && (c7 == 'I' || c7 == 'i') && (c8 == 'O' || c8 == 'o') && (c9 == 'N' || c9 == 'n')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {

                return CONNECTION;

            }
        }
        return OTHER;
    }
    
    static final int engineCheck(final String stmt, final int offset){
    	if(stmt.length() > offset + "NGINES".length()){
    		int i = offset;
    		final char c1 = stmt.charAt(++i);
    		final char c2 = stmt.charAt(++i);
    		final char c3 = stmt.charAt(++i);
    		final char c4 = stmt.charAt(++i);
    		final char c5 = stmt.charAt(++i);
    		final char c6 = stmt.charAt(++i);
    		if( (c1 == 'N' || c1 == 'n') && 
    			(c2 == 'G' || c2 == 'g') && 
    			(c3 == 'I' || c3 == 'i') &&
    			(c4 == 'N' || c4 == 'n') &&
    			(c5 == 'E' || c5 == 'e') && 
    			(c6 == 'S' || c6 == 's') &&
    			(stmt.length() == ++i || ParseUtil.isEOF(stmt.charAt(i)))
    		){
    			return ENGINES;
    		}
    	}
    	return OTHER;
    }

    private static int slowCheck(String stmt, int offset) {
        if (stmt.length() > offset + "low".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'O' || c2 == 'o') && (c3 == 'W' || c3 == 'w')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return SLOW;
            }
        }
        return OTHER;
    }

    private static int physicalCheck(String stmt, int offset) {
        if (stmt.length() > offset + "hysical_slow".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            char c9 = stmt.charAt(++offset);
            char c10 = stmt.charAt(++offset);
            char c11 = stmt.charAt(++offset);
            char c12 = stmt.charAt(++offset);
            if ((c1 == 'h' || c1 == 'H') && (c2 == 'y' || c2 == 'Y') && (c3 == 's' || c3 == 'S')

            && (c4 == 'i' || c4 == 'I') && (c5 == 'c' || c5 == 'C') && (c6 == 'a' || c6 == 'A')
                && (c7 == 'l' || c7 == 'L') && (c8 == '_') && (c9 == 's' || c9 == 'S') && (c10 == 'l' || c10 == 'L')
                && (c11 == 'o' || c11 == 'O') && (c12 == 'w' || c12 == 'W')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return PHYSICAL_SLOW;

            }
        }
        return OTHER;
    }

    // SHOW COBAR_
    static int cobarCheck(String stmt, int offset) {
        if (stmt.length() > offset + "obar_?".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'B' || c2 == 'b') && (c3 == 'A' || c3 == 'a')
                && (c4 == 'R' || c4 == 'r') && (c5 == '_')) {
                switch (stmt.charAt(++offset)) {
                    case 'S':
                    case 's':
                        return showCobarStatus(stmt, offset);
                    case 'C':
                    case 'c':
                        return showCobarCluster(stmt, offset);
                    default:
                        return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SHOW COBAR_STATUS
    static int showCobarStatus(String stmt, int offset) {
        if (stmt.length() > offset + "tatus".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            if ((c1 == 't' || c1 == 'T') && (c2 == 'a' || c2 == 'A') && (c3 == 't' || c3 == 'T')
                && (c4 == 'u' || c4 == 'U') && (c5 == 's' || c5 == 'S')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return COBAR_STATUS;
            }
        }
        return OTHER;
    }

    // SHOW COBAR_CLUSTER
    static int showCobarCluster(String stmt, int offset) {
        if (stmt.length() > offset + "luster".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'L' || c1 == 'l') && (c2 == 'U' || c2 == 'u') && (c3 == 'S' || c3 == 's')
                && (c4 == 'T' || c4 == 't') && (c5 == 'E' || c5 == 'e') && (c6 == 'R' || c6 == 'r')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return COBAR_CLUSTER;
            }
        }
        return OTHER;
    }

    // SHOW DATA
    static int dataCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ata?".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'T' || c2 == 't') && (c3 == 'A' || c3 == 'a')) {
                switch (stmt.charAt(++offset)) {
                    case 'B':
                    case 'b':
                        return showDatabases(stmt, offset);
                    case 'S':
                    case 's':
                        return showDataSources(stmt, offset);
                    default:
                        return OTHER;
                }
            }
        }
        return OTHER;
    }

    // SHOW DATABASES
    static int showDatabases(String stmt, int offset) {
        if (stmt.length() > offset + "ases".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'S' || c2 == 's') && (c3 == 'E' || c3 == 'e')
                && (c4 == 'S' || c4 == 's') && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return DATABASES;
            }
        }
        return OTHER;
    }

    // SHOW DATASOURCES
    static int showDataSources(String stmt, int offset) {
        if (stmt.length() > offset + "ources".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            if ((c1 == 'O' || c1 == 'o') && (c2 == 'U' || c2 == 'u') && (c3 == 'R' || c3 == 'r')
                && (c4 == 'C' || c4 == 'c') && (c5 == 'E' || c5 == 'e') && (c6 == 'S' || c6 == 's')
                && (stmt.length() == ++offset || ParseUtil.isEOF(stmt.charAt(offset)))) {
                return DATASOURCES;
            }
        }
        return OTHER;
    }
    
    // SHOW VARIABLES
    static int showVCheck(String stmt, int offset) {
        if (stmt.length() > offset + "ARIABLES".length()) {
            char c1 = stmt.charAt(++offset);
            char c2 = stmt.charAt(++offset);
            char c3 = stmt.charAt(++offset);
            char c4 = stmt.charAt(++offset);
            char c5 = stmt.charAt(++offset);
            char c6 = stmt.charAt(++offset);
            char c7 = stmt.charAt(++offset);
            char c8 = stmt.charAt(++offset);
            if ((c1 == 'A' || c1 == 'a') && (c2 == 'R' || c2 == 'r') && (c3 == 'I' || c3 == 'i')
                && (c4 == 'A' || c4 == 'a') && (c5 == 'B' || c5 == 'b') && (c6 == 'L' || c6 == 'l')
                && (c7 == 'E' || c7 == 'e') && (c8 == 'S' || c8 == 's')) {
                if (stmt.length() > ++offset && stmt.charAt(offset) != ' ') {
                    return OTHER;
                }
                return VARIABLES;
            }
        }
        return OTHER;
    }
    // SHOW GLOBLE VARIABLES
    static int showGVCheck(String stmt, int offset) {
        if (stmt.length() > offset + "GLOBLE ".length()) {
            return showVCheck(stmt, offset + "GLOBLE ".length());
        }
        return OTHER;
    }
    // SHOW SESSION VARIABLES
    static int showSVCheck(String stmt, int offset) {
        String s = stmt.substring(offset).toLowerCase();
        if (s.startsWith("session variables")) {
            return SESSION_VARIABLES;
        } if (s.startsWith("session status")) {
            return SESSION_STATUS;
        } else {
            return OTHER;
        }
    }

}
