package com.diven.hive.blood.enums;

public interface Constants {
      String SPLIT_DOT = ".";
      String SPLIT_COMMA = ",";
      String SPLIT_AND = "&";
      String TOK_EOF = "<EOF>";
      String CON_WHERE = "WHERE:";
      String TOK_TMP_FILE = "TOK_TMP_FILE";
      String regex_replace_params = "\\$\\{.*?\\}|\\$[a-zA-Z_]+[a-z-A-Z_0-9]+"; 	/**变量替换正则*/
     
      String regex_replace_exclude = "`\\(.*?\\).+?`"; 	/**排除列替换正则*/
}
