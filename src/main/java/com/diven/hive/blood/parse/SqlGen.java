package com.diven.hive.blood.parse;

import com.diven.hive.blood.enums.CodeType;
import com.diven.hive.blood.model.*;
import com.diven.hive.blood.utils.Check;
import com.diven.hive.blood.model.Column;
import com.diven.hive.blood.model.Select;
import lombok.Data;

import java.util.*;

@Data
public class SqlGen {
    HashMap<String, Select> curr = new HashMap<>();
    HashMap<String, String> currSql = new HashMap<>();

    public String genSql(List<Select> str) {
        String selectSql;
        for (Select st : str) {
            curr.put(st.getCurrent(), st);
            st.setColSelect(true);
            List<Column> cols = new ArrayList<>();
            cols.add(st.getColumnList().get(2));
            selectSql = genSelect(st, cols);
            currSql.put(st.getCurrent(), selectSql);
            System.out.println(selectSql);
        }

        return null;

    }


    public String genSelect(Select select, List<Column> columns) {
        StringBuilder selectSql;
        selectSql = new StringBuilder(" select ");
        Set<String> baseTables = new HashSet<>();
        Map<String, HashSet<String>> colSelect = new HashMap<String, HashSet<String>>();
        Map<String, ArrayList<Column>> subColumn = new HashMap<String, ArrayList<Column>>();

        // 寻找需要的 字段
        for (Column colTmp : columns) {

            for (String baseColName : colTmp.getBaseColSet()) {
                // 表名
                String str = baseColName.substring(0, baseColName.lastIndexOf("."));

                // 添加字段
                if (colSelect.containsKey(str)) {
                    colSelect.get(str).add(baseColName.substring(baseColName.lastIndexOf(".") + 1));
                } else {
                    HashSet<String> tmp = new HashSet<>();
                    tmp.add(baseColName.substring(baseColName.lastIndexOf(".") + 1));
                    colSelect.put(str, tmp);
                }
            }

            // 需要哪些字表
            baseTables.addAll(colTmp.getBaseTableSet());
            if (!Check.isEmpty(colTmp.getToNameParse())) {
                selectSql.append("\n," + colTmp.getBaseExpr() + " as " + colTmp.getToNameParse());
            }
        }


        if (select.getCodeType().equals(CodeType.TABLE)) {
            selectSql.append("\n from ");
            selectSql.append(select.getCurrent() + " ");
            if (select.getWhere() != null) {
                selectSql.append("\n where " + select.getWhere().getWhereExpr() + "\n");
            }
        } else {
            selectSql.append("\n from ( ");
            List<Select> subSelects = getIndexSelect(select, baseTables);
            if (subSelects.size() != 0) {
                for (int i = 0; i <= subSelects.size() - 1; i++) {
                    Select first = subSelects.get(i);
                    Select second;
                    if (i == subSelects.size() - 1) {
                        second = null;
                    } else {
                        second = subSelects.get(i + 1);
                    }
                    ArrayList<Column> tmp = new ArrayList<>();
                    HashSet<String> colName = colSelect.get(first.getCurrent());
                    for (Column col : first.getColumnList()) {
                        if (colName != null && (colName.contains(col.getToNameParse()) || colName.contains(col.getBaseExpr()))) {
                            tmp.add(col);
                        }
                    }
                    String temp = genSelect(first, tmp);
                    if (select.getColSelect()) {
                        selectSql.append(temp);
                        // 后面有跟着的
                        if (second != null) {
                            if (second.isUnion()) {
                                selectSql.append(" union all\n");
                            } else if (second.getJoin() != null) {
                                selectSql.append(second.getJoin().getJoinType().name());
                            }
                        } else {
                            selectSql.append(") " + select.getCurrent());
                        }
                    } else {
                        selectSql.append("  )" + select.getCurrent());
                    }
                    subColumn.put(first.getCurrent(), tmp);
                }
            }
        }

        // System.out.println(selectSql);
        return selectSql.toString();
    }

    public List<Select> getIndexSelect(Select select, Set<String> baseTable) {
        List<Select> res = new ArrayList<>();
        for (Select sl : select.getChildList()) {
            if (baseTable.contains(sl.getCurrent())) {
                res.add(sl);
            }

        }
        return res;
    }

}
