package com.openddal.server.mysql.respo;

import java.util.regex.Pattern;

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 */
public final class ShowTables {

    private static final Pattern SHOW_TABLES = Pattern
            .compile("\\s*show(\\s+full)?\\s+tables\\s*([from|in]\\s+[a-zA-Z_0-9]+)?");

    public static boolean isShowTables(String sql) {
        return SHOW_TABLES.matcher(sql).find();
    }


}