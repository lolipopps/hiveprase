package com.diven.hive.blood.parse;

import com.diven.hive.blood.enums.CodeType;
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
            cols.add(st.getColumnList().get(20));
            selectSql = genSelect(st, cols, 0);
            currSql.put(st.getCurrent(), selectSql);
            System.out.println(selectSql);
        }
        return null;

    }


    public String genSelect(Select select, List<Column> columns, int num) {
        StringBuilder space = new StringBuilder();
        for (int i = 0; i < num; i++) {
            space.append(" ");
        }
        StringBuilder selectSql = new StringBuilder();
        Set<String> needTables = new HashSet<>();
        Map<String, HashSet<String>> needCols = new HashMap<String, HashSet<String>>();
        Map<String, ArrayList<Column>> subColumn = new HashMap<String, ArrayList<Column>>();
        selectSql.append(space.toString()+"select ");
        // 寻找需要的 字段

        for (int i = 0; i < columns.size(); i++) {
            Column colTmp = columns.get(i);
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
            Stack<String> stack = new Stack<>();
            for (String baseTabel : colTmp.getBaseTableSet()) {
                stack.add(baseTabel);
                while (stack.size() != 0) {
                    String tmpTable = stack.pop();
                    for (Select select1 : select.getChildList()) {
                        if (select1.getCurrent() == tmpTable) {
                            // 处理关联表
                            if (select1.getJoin() != null) {
                                for (String joinTable : select1.getJoin().getBaseTableSet()) {
                                    if (!needTables.contains(joinTable)) {
                                        needTables.add(joinTable);
                                        stack.add(joinTable);
                                    }
                                }
                                // 处理关联字段
                                for (String baseColName : select1.getJoin().getBaseColSet()) {
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

                            }

                        }
                    }
                }
            }
            // 拼出该层sql
            if (!Check.isEmpty(colTmp.getToNameParse())) {
                if (i == 0) {
                    selectSql.append(" "+colTmp.getBaseExpr() + " as " + colTmp.getToNameParse());

                } else {
                    selectSql.append("\n"+space.toString() + "," + colTmp.getBaseExpr() + " as " + colTmp.getToNameParse());
                }
            }
        }

        // 是不是基础表
        if (select.getChildList().size() == 0) {
            selectSql.append("\n" + space.toString() + "from ");
            selectSql.append(select.getBaseTableSet().toString().replace("[", "").replace("]", "") + " ");
            if (select.getWhere() != null) {
                selectSql.append("\n" + space.toString() + "where " + select.getWhere().getWhereExpr() + "\n");
            }
        } else { // 否则是个子查询
            selectSql.append("\n" + space.toString() + "from ( \n");
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
                String temp = genSelect(first, tmp, num + 4);
                // 需要拼出字段
                selectSql.append(temp + ")" + first.getAlias() + " \n");
                // 后面有跟着的
                if (first.getJoin() != null) {
                    selectSql.append(space.toString() + "on  " + first.getJoin().getJoinExpr() + "\n");
                }
                if (second != null) {
                    if (second.isUnion()) {
                        selectSql.append(" union all\n");
                    } else if (second.getJoin() != null) {
                        selectSql.append(second.getJoin().getJoinType().getName() + " ( \n");
                    }
                }
                subColumn.put(first.getCurrent(), tmp);
            }
            if (select.getWhere() != null) {
                selectSql.append(" where " + select.getWhere().getWhereExpr());
            }
        }
        // System.out.println(selectSql);
        return selectSql.toString().replace("select \n,", "select\n");
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
