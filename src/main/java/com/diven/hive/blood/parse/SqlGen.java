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
            cols.add(st.getColumnList().get(0));
            selectSql = genSelect(st, cols);
            currSql.put(st.getCurrent(), selectSql);
            System.out.println(selectSql);
        }

        return null;

    }


    public String genSelect(Select select, List<Column> columns) {
        StringBuilder selectSql = new StringBuilder();
        Set<String> needTables = new HashSet<>();
        Map<String, HashSet<String>> needCols = new HashMap<String, HashSet<String>>();
        Map<String, ArrayList<Column>> subColumn = new HashMap<String, ArrayList<Column>>();
        if (select.getColSelect() || select.getId() == 0) {
            selectSql.append(" select ");
        }
        // 寻找需要的 字段
        for (Column colTmp : columns) {

            for (String baseColName : colTmp.getBaseColSet()) {
                // 表名
                String str = baseColName.substring(0, baseColName.lastIndexOf("."));
                // 添加字段
                if (needCols.containsKey(str)) {
                    needCols.get(str).add(baseColName.substring(baseColName.lastIndexOf(".") + 1));
                } else {
                    HashSet<String> tmp = new HashSet<>();
                    tmp.add(baseColName.substring(baseColName.lastIndexOf(".") + 1));
                    needCols.put(str, tmp);
                }
            }

            // 需要哪些字表
            needTables.addAll(colTmp.getBaseTableSet());

            if (select.getColSelect() || select.getId() == 0) {
                // 拼出改层sql
                if (!Check.isEmpty(colTmp.getToNameParse())) {
                    selectSql.append("\n," + colTmp.getBaseExpr() + " as " + colTmp.getToNameParse());
                }
            }
        }


        // 是不是基础表
        if (select.getCodeType().equals(CodeType.TABLE)) {
            selectSql.append("\n from ");
            selectSql.append(select.getCurrent() + " ");
            if (select.getWhere() != null) {
                selectSql.append("\n where " + select.getWhere().getWhereExpr() + "\n");
            }
        } else { // 否则是个子查询
            if (select.getColSelect() || select.getId() == 0) {
                selectSql.append("\n from ( ");
            }
            // 按顺序 找出需要的子查询
            List<Select> subSelects = getIndexSelect(select, needTables);

            //   if (subSelects.size() != 0) { // 存在子查询
            // 遍历子查询
            for (int i = 0; i <= subSelects.size() - 1; i++) {
                Select first = subSelects.get(i);
                Select second;

                // 是不是最后一个 防止数组越界
                if (i == subSelects.size() - 1) {
                    second = null;
                } else {
                    second = subSelects.get(i + 1);
                }
                ArrayList<Column> tmp = new ArrayList<>();

                // 查看这个子查询需要哪些字段
                HashSet<String> colName = needCols.get(first.getCurrent());

                // 找出需要的字段 准备递归
                for (Column col : first.getColumnList()) {
                    if (colName != null && (colName.contains(col.getToNameParse()) || colName.contains(col.getBaseExpr()))) {
                        tmp.add(col);
                    }
                }

                String temp = genSelect(first, tmp);

                // 需要拼出字段
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
                subColumn.put(first.getCurrent(), tmp);
            }
            // }
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
