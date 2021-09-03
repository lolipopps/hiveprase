package com.diven.hive.blood.utils;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class HqlUtil {

	/**
	 * 判断正常列，
	 * 正常：a as col, a
	 * 异常：1 ，'a' //数字、字符等作为列名
	 */
	public static boolean notNormalCol(String column) {
		return Check.isEmpty(column) || NumberUtil.isNumeric(column)
				|| (column.startsWith("\"") && column.endsWith("\""))
				|| (column.startsWith("\'") && column.endsWith("\'"));
	}

	/**
	 * 	过滤掉无用的列：如col1,123,'2013',col2 ==>> col1,col2
	 * @param colSet
	 * @return
	 */
	public static Set<String> filterData(Set<String> colSet){
		Set<String> set  = new LinkedHashSet<String>();
		for (String string : colSet) {
			if (!notNormalCol(string)) {
				set.add(string);
			}
		}
		return set;
	}


	public static String ListToString(List<String> hqls) {
		StringBuffer buffer = new StringBuffer();
		if(hqls != null) {
			for(String sql : hqls) {
				sql = sql.trim();
				boolean flag = false;
				for(String line : sql.split("\n")) {
					if(line.trim().startsWith("--")) {
						continue;
					}
					if(!line.trim().isEmpty()) {
						flag = true;
						buffer.append(line).append("\n");	
					}
				}
				if(flag && !sql.endsWith(";")) {
					buffer.append(";");
				}
			}
		}
		return buffer.toString();
	}
	
}
