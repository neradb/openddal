package com.openddal.server.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xianmao.hexm 2010-8-3 下午06:12:53
 */
public class CharsetUtil {

    private static final String[]             INDEX_TO_CHARSET = new String[255];
    private static final Map<String, Integer> CHARSET_TO_INDEX = new HashMap<String, Integer>();
    static {
        // index --> charset
        INDEX_TO_CHARSET[1] = "big5";
        INDEX_TO_CHARSET[2] = "czech";
        INDEX_TO_CHARSET[3] = "dec8";
        INDEX_TO_CHARSET[4] = "dos";
        INDEX_TO_CHARSET[5] = "german1";
        INDEX_TO_CHARSET[6] = "hp8";
        INDEX_TO_CHARSET[7] = "koi8_ru";
        INDEX_TO_CHARSET[8] = "latin1";
        INDEX_TO_CHARSET[9] = "latin2";
        INDEX_TO_CHARSET[10] = "swe7";
        INDEX_TO_CHARSET[11] = "usa7";
        INDEX_TO_CHARSET[12] = "ujis";
        INDEX_TO_CHARSET[13] = "sjis";
        INDEX_TO_CHARSET[14] = "cp1251";
        INDEX_TO_CHARSET[15] = "danish";
        INDEX_TO_CHARSET[16] = "hebrew";
        INDEX_TO_CHARSET[18] = "tis620";
        INDEX_TO_CHARSET[19] = "euc_kr";
        INDEX_TO_CHARSET[20] = "estonia";
        INDEX_TO_CHARSET[21] = "hungarian";
        INDEX_TO_CHARSET[22] = "koi8_ukr";
        INDEX_TO_CHARSET[23] = "win1251ukr";
        INDEX_TO_CHARSET[24] = "gb2312";
        INDEX_TO_CHARSET[25] = "greek";
        INDEX_TO_CHARSET[26] = "win1250";
        INDEX_TO_CHARSET[27] = "croat";
        INDEX_TO_CHARSET[28] = "gbk";
        INDEX_TO_CHARSET[29] = "cp1257";
        INDEX_TO_CHARSET[30] = "latin5";
        INDEX_TO_CHARSET[31] = "latin1_de";
        INDEX_TO_CHARSET[32] = "armscii8";
        INDEX_TO_CHARSET[33] = "utf8";
        INDEX_TO_CHARSET[34] = "win1250ch";
        INDEX_TO_CHARSET[35] = "ucs2";
        INDEX_TO_CHARSET[36] = "cp866";
        INDEX_TO_CHARSET[37] = "keybcs2";
        INDEX_TO_CHARSET[38] = "macce";
        INDEX_TO_CHARSET[39] = "macroman";
        INDEX_TO_CHARSET[40] = "pclatin2";
        INDEX_TO_CHARSET[41] = "latvian";
        INDEX_TO_CHARSET[42] = "latvian1";
        INDEX_TO_CHARSET[43] = "maccebin";
        INDEX_TO_CHARSET[44] = "macceciai";
        INDEX_TO_CHARSET[45] = "utf8mb4";
        INDEX_TO_CHARSET[46] = "utf8mb4";
        INDEX_TO_CHARSET[47] = "latin1bin";
        INDEX_TO_CHARSET[48] = "latin1cias";
        INDEX_TO_CHARSET[49] = "latin1csas";
        INDEX_TO_CHARSET[50] = "cp1251bin";
        INDEX_TO_CHARSET[51] = "cp1251cias";
        INDEX_TO_CHARSET[52] = "cp1251csas";
        INDEX_TO_CHARSET[53] = "macromanbin";
        INDEX_TO_CHARSET[54] = "macromancias";
        INDEX_TO_CHARSET[55] = "macromanciai";
        INDEX_TO_CHARSET[56] = "macromancsas";
        INDEX_TO_CHARSET[57] = "cp1256";
        INDEX_TO_CHARSET[63] = "binary";
        INDEX_TO_CHARSET[64] = "armscii";
        INDEX_TO_CHARSET[65] = "ascii";
        INDEX_TO_CHARSET[66] = "cp1250";
        INDEX_TO_CHARSET[67] = "cp1256";
        INDEX_TO_CHARSET[68] = "cp866";
        INDEX_TO_CHARSET[69] = "dec8";
        INDEX_TO_CHARSET[70] = "greek";
        INDEX_TO_CHARSET[71] = "hebrew";
        INDEX_TO_CHARSET[72] = "hp8";
        INDEX_TO_CHARSET[73] = "keybcs2";
        INDEX_TO_CHARSET[74] = "koi8r";
        INDEX_TO_CHARSET[75] = "koi8ukr";
        INDEX_TO_CHARSET[77] = "latin2";
        INDEX_TO_CHARSET[78] = "latin5";
        INDEX_TO_CHARSET[79] = "latin7";
        INDEX_TO_CHARSET[80] = "cp850";
        INDEX_TO_CHARSET[81] = "cp852";
        INDEX_TO_CHARSET[82] = "swe7";
        INDEX_TO_CHARSET[83] = "utf8";
        INDEX_TO_CHARSET[84] = "big5";
        INDEX_TO_CHARSET[85] = "euckr";
        INDEX_TO_CHARSET[86] = "gb2312";
        INDEX_TO_CHARSET[87] = "gbk";
        INDEX_TO_CHARSET[88] = "sjis";
        INDEX_TO_CHARSET[89] = "tis620";
        INDEX_TO_CHARSET[90] = "ucs2";
        INDEX_TO_CHARSET[91] = "ujis";
        INDEX_TO_CHARSET[92] = "geostd8";
        INDEX_TO_CHARSET[93] = "geostd8";
        INDEX_TO_CHARSET[94] = "latin1";
        INDEX_TO_CHARSET[95] = "cp932";
        INDEX_TO_CHARSET[96] = "cp932";
        INDEX_TO_CHARSET[97] = "eucjpms";
        INDEX_TO_CHARSET[98] = "eucjpms";
        // 其他编码
        INDEX_TO_CHARSET[99] = "cp1250";
        INDEX_TO_CHARSET[100] = "latin1";
        for (int i = 101; i <= 124; i++) {
            INDEX_TO_CHARSET[i] = "utf16";
        }
        for (int i = 125; i <= 127; i++) {
            INDEX_TO_CHARSET[i] = "latin1";
        }
        for (int i = 128; i <= 151; i++) {
            INDEX_TO_CHARSET[i] = "ucs2";
        }
        for (int i = 152; i <= 158; i++) {
            INDEX_TO_CHARSET[i] = "latin1";
        }
        INDEX_TO_CHARSET[159] = "ucs2";
        for (int i = 160; i <= 183; i++) {
            INDEX_TO_CHARSET[i] = "utf32";
        }
        for (int i = 184; i <= 191; i++) {
            INDEX_TO_CHARSET[i] = "latin1";
        }
        for (int i = 192; i <= 215; i++) {
            INDEX_TO_CHARSET[i] = "utf8";
        }
        for (int i = 216; i <= 222; i++) {
            INDEX_TO_CHARSET[i] = "latin1";
        }
        INDEX_TO_CHARSET[223] = "utf8";
        for (int i = 224; i <= 247; i++) {
            INDEX_TO_CHARSET[i] = "utf8mb4";
        }
        for (int i = 248; i <= 253; i++) {
            INDEX_TO_CHARSET[i] = "latin1";
        }
        INDEX_TO_CHARSET[254] = "utf8";

        // charset --> index
        for (int i = 0; i < 99; i++) {
            String charset = INDEX_TO_CHARSET[i];
            if (charset != null && CHARSET_TO_INDEX.get(charset) == null) {
                CHARSET_TO_INDEX.put(charset, i);
            }
        }
        CHARSET_TO_INDEX.put("iso-8859-1", 14);
        CHARSET_TO_INDEX.put("iso_8859_1", 14);
        CHARSET_TO_INDEX.put("utf-8", 33);
        CHARSET_TO_INDEX.put("utf8mb4", 45);
    }

    public static final String getCharset(int index) {
        return INDEX_TO_CHARSET[index];
    }

    public static final int getIndex(String charset) {
        if (charset == null || charset.length() == 0) {
            return 0;
        } else {
            Integer i = CHARSET_TO_INDEX.get(charset.toLowerCase());
            return (i == null) ? 0 : i.intValue();
        }
    }

    public static String getJavaCharset(String charset) {
        if (endsWith(charset, "utf8mb4", true)) {
            return "utf-8";
        }

        return charset;
    }
    
    public static boolean endsWith(String str, String suffix, boolean ignoreCase) {
        if (str == null || suffix == null) {
            return (str == null && suffix == null);
        }
        if (suffix.length() > str.length()) {
            return false;
        }
        int strOffset = str.length() - suffix.length();
        return str.regionMatches(ignoreCase, strOffset, suffix, 0, suffix.length());
    }
}
