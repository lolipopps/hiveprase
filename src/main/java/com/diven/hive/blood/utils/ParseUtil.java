package com.diven.hive.blood.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.diven.hive.blood.exception.SQLParseException;
import com.diven.hive.blood.model.Block;
import com.diven.hive.blood.model.Column;
import com.diven.hive.blood.model.ColumnBase;
import com.diven.hive.blood.model.Select;
import com.diven.hive.ql.parse.ASTNode;
import com.diven.hive.ql.parse.BaseSemanticAnalyzer;
import com.diven.hive.ql.parse.HiveParser;
import org.antlr.runtime.tree.Tree;


/**
 * 解析工具类
 *
 * @author divenwu
 */
public final class ParseUtil {
    private static final Map<Integer, String> hardcodeScriptMap = new HashMap<Integer, String>();
    private static final String SPLIT_DOT = ".";
    private static final String SPLIT_COMMA = ",";
    private static final Map<String, Boolean> REGEX_MULTI_VAR_VALUE = new HashMap<String, Boolean>();

    static {
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*\"([\\s\\S]*?)\"", false);
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*'([\\s\\S]*?)'", false);
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*(\\w*)", false);
        REGEX_MULTI_VAR_VALUE.put("\\s*=\\s*`([\\s\\S]*?)`", true);
        hardcodeScriptMap.put(400, "^\\s*hive\\d?.*?-e\\s*\"([\\s\\S]*)\"");
    }

    private ParseUtil() {
    }

    /**
     * @param table fact.t1
     * @return [fact, t1]
     */
    public static String[] parseDBTable(String table) {
        return table.split("\\" + SPLIT_DOT);
    }

    public static String collectionToString(Collection<String> coll) {
        return collectionToString(coll, SPLIT_COMMA, true);
    }

    public static String collectionToString(Collection<String> coll, String split, boolean isCheck) {
        StringBuilder sb = new StringBuilder();
        if (Check.notEmpty(coll)) {
            for (String string : coll) {
                if (!isCheck || Check.notEmpty(string)) {
                    sb.append(string).append(split);
                }
            }
            if (sb.length() > 0) {
                sb.setLength(sb.length() - split.length());
            }
        }
        return sb.toString();
    }

    public static String uniqMerge(String s1, String s2) {
        Set<String> set = new HashSet<String>();
        set.add(s1);
        set.add(s2);
        return collectionToString(set);
    }

    public static String escape(String keyword) {
        if (Check.notEmpty(keyword)) {
            String[] fbsArr = {"\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|"};
            for (String key : fbsArr) {
                if (keyword.contains(key)) {
                    keyword = keyword.replace(key, "\\" + key);
                }
            }
        }
        return keyword;
    }

    public static Map<String, String> cloneAliaMap(Map<String, String> map) {
        Map<String, String> map2 = new HashMap<String, String>(map.size());
        for (Entry<String, String> entry : map.entrySet()) {
            map2.put(entry.getKey(), entry.getValue());
        }
        return map2;
    }

    public static Map<String, List<Column>> cloneSubQueryMap(Map<String, List<Column>> map) {
        Map<String, List<Column>> map2 = new HashMap<String, List<Column>>(map.size());
        for (Entry<String, List<Column>> entry : map.entrySet()) {
            List<Column> value = entry.getValue();
            List<Column> list = new ArrayList<Column>(value.size());
            for (Column column : value) {
                list.add(cloneColLine(column));
            }
            map2.put(entry.getKey(), value);
        }
        return map2;
    }


    /**
     * 查找当前节点的父子查询节点
     *
     * @param ast
     */
    public static Select getSubQueryParent(Tree ast) {
        Tree _tree = ast;
        Select qt = new Select();
        while (!(_tree = _tree.getParent()).isNil()) {
            if (_tree.getType() == HiveParser.TOK_SUBQUERY || _tree.getType() == HiveParser.TOK_QUERY ) {
                qt.setPid(generateTreeId(_tree));
                qt.setId(generateTreeId(_tree));
                qt.setParent(BaseSemanticAnalyzer.getUnescapedName((ASTNode) _tree.getChild(1)));
                return qt;
            }
        }
        qt.setPid(0);
        qt.setParent("NIL");
        return qt;
    }

    public static Select getQueryParent(Tree ast) {
        Tree _tree = ast;
        Select qt = new Select();
        while (!(_tree = _tree.getParent()).isNil()) {
            if (_tree.getType() == HiveParser.TOK_QUERY) {
                qt.setPid(generateTreeId(_tree));
                qt.setParent(BaseSemanticAnalyzer.getUnescapedName((ASTNode) _tree.getChild(1)));
                return qt;
            }
        }
        qt.setPid(0);
        qt.setParent("NIL");
        return qt;
    }

    public static Integer getFromId(Tree ast) {
        Tree _tree = ast;
        while (!(_tree = _tree.getParent()).isNil()) {
            if (_tree.getType() == HiveParser.TOK_FROM) {
                return generateTreeId(_tree);
            }
        }
        return 0;
    }

    public static Integer getSubQueryParentId(Tree ast) {
        Tree _tree = ast;
        while (!(_tree = _tree.getParent()).isNil()) {
            if (_tree.getType() == HiveParser.TOK_SUBQUERY || _tree.getType() == HiveParser.TOK_QUERY ) {
                return generateTreeId(_tree);
            }
        }
        return 0;
    }

    public static Integer getQueryParentId(Tree ast) {
        Tree _tree = ast;
        while (!(_tree = _tree.getParent()).isNil()) {
            if (_tree.getType() == HiveParser.TOK_QUERY) {
                return generateTreeId(_tree);
            }
        }
        return -1;
    }

    public static Integer getQueryChildId(Tree ast) {
        Tree _tree = ast;
        while (!(_tree = _tree.getChild(0)).isNil()) {
            if (_tree.getType() == HiveParser.TOK_QUERY) {
                return generateTreeId(_tree);
            }
        }
        return -1;
    }


    public static int generateTreeId(Tree tree) {
        return tree.getTokenStartIndex() + tree.getTokenStopIndex();
    }


    public static Column cloneColLine(Column col) {
        Column newCol = new Column();
        newCol.setToNameParse(col.getToNameParse());
        newCol.setToTable(col.getToTable());
        newCol.setColCondition(col.getColCondition());
        newCol.setBaseExpr(col.getBaseExpr());
        newCol.setColSet(col.getColSet());
        newCol.setBaseColSet(col.getBaseColSet());
        newCol.setTableSet(col.getTableSet());
        newCol.setBaseTableSet(col.getBaseTableSet());
        newCol.setAllColSet(col.getAllColSet());
        newCol.setAllTableSet(col.getAllTableSet());
        return newCol;

    }



    public static void BlockToColumn(Block block, ColumnBase columnBase) {
        columnBase.getBaseColSet().addAll(block.getBaseColSet());
        columnBase.getColSet().addAll(block.getColSet());
        columnBase.getBaseTableSet().addAll(block.getBaseTableSet());
        columnBase.getTableSet().addAll(block.getTableSet());
    }


    public static Set<String> cloneSet(Set<String> set) {
        Set<String> set2 = new HashSet<String>(set.size());
        for (String string : set) {
            set2.add(string);
        }
        return set2;
    }

    public static List<Column> cloneList(List<Column> list) {
        List<Column> list2 = new ArrayList<Column>(list.size());
        for (Column col : list) {
            list2.add(cloneColLine(col));
        }
        return list2;
    }


    /**
     * 校验union
     *
     * @param list
     */
    public static void validateUnion(List<Column> list) {
        int size = list.size();
        if (size % 2 == 1) {
            throw new SQLParseException("union column number are different, size=" + size);
        }
        int colNum = size / 2;
        checkUnion(list, 0, colNum);
        checkUnion(list, colNum, size);
    }

    public static void checkUnion(List<Column> list, int start, int end) {
        String tmp = null;
        for (int i = start; i < end; i++) { //合并字段
            Column col = list.get(i);
            if (Check.isEmpty(tmp)) {
                tmp = col.getToTable();
            } else if (!tmp.equals(col.getToTable())) {
                throw new SQLParseException("union column number/types are different,table1=" + tmp + ",table2=" + col.getToTable());
            }
        }
    }



}
