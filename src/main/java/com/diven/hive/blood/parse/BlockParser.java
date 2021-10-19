package com.diven.hive.blood.parse;

import com.diven.hive.blood.enums.CodeType;
import com.diven.hive.blood.exception.SQLParseException;
import com.diven.hive.blood.model.Block;
import com.diven.hive.blood.model.Column;
import com.diven.hive.blood.model.Group;
import com.diven.hive.blood.model.Select;
import com.diven.hive.blood.utils.Check;
import com.diven.hive.blood.utils.MetaCache;
import com.diven.hive.blood.utils.ParseUtil;
import com.diven.hive.ql.parse.ASTNode;
import com.diven.hive.ql.parse.BaseSemanticAnalyzer;
import com.diven.hive.ql.parse.HiveParser;
import com.diven.hive.blood.enums.Constants;
import lombok.Data;
import org.antlr.runtime.tree.Tree;

import java.util.*;

@Data
public class BlockParser {

    public LinkedHashMap<String, Select> queryMap;

    public Set<String> inputTables;

    public StringBuilder nowQueryDB;

    /**
     * 获得解析的块，主要应用在WHERE、JOIN和SELECT端
     * 如： <p>where a=1
     * <p>t1 join t2 on t1.col1=t2.col1 and t1.col2=123
     * <p>select count(distinct col1) from t1
     *
     * @param ast
     * @return
     */
    public Block getBlockIteral(ASTNode ast) {
        if (ast.getType() == HiveParser.KW_OR // 处理 or 或者 and 需要拼起来
                || ast.getType() == HiveParser.KW_AND) {

            Block bk1 = getBlockIteral((ASTNode) ast.getChild(0));
            Block bk2 = getBlockIteral((ASTNode) ast.getChild(1));

            bk1.getColSet().addAll(bk2.getColSet());
            bk1.getBaseColSet().addAll(bk2.getBaseColSet());

            bk1.getAllColSet().addAll(bk2.getAllColSet());
            bk1.getAllTableSet().addAll(bk2.getAllTableSet());

            bk1.getTableSet().addAll(bk2.getTableSet());
            bk1.getBaseTableSet().addAll(bk2.getBaseTableSet());

            bk1.setCondition("(" + bk1.getCondition() + " " + ast.getText() + " " + bk2.getCondition() + ")");
            bk1.setBaseExpr(" " + bk1.getBaseExpr() + " " + ast.getText() + " " + bk2.getBaseExpr() + " ");

            return bk1;
        } else if (ast.getType() == HiveParser.NOTEQUAL //判断条件  > < like in + = 等各种符号 not
                || ast.getType() == HiveParser.EQUAL
                || ast.getType() == HiveParser.LESSTHAN
                || ast.getType() == HiveParser.LESSTHANOREQUALTO
                || ast.getType() == HiveParser.GREATERTHAN
                || ast.getType() == HiveParser.GREATERTHANOREQUALTO
                || ast.getType() == HiveParser.KW_LIKE
                || ast.getType() == HiveParser.DIVIDE
                || ast.getType() == HiveParser.PLUS
                || ast.getType() == HiveParser.MINUS
                || ast.getType() == HiveParser.STAR
                || ast.getType() == HiveParser.MOD
                || ast.getType() == HiveParser.AMPERSAND
                || ast.getType() == HiveParser.TILDE
                || ast.getType() == HiveParser.BITWISEOR
                || ast.getType() == HiveParser.BITWISEXOR || ast.getType() == HiveParser.KW_NOT) {

            Block bk1 = getBlockIteral((ASTNode) ast.getChild(0));

            if (ast.getChild(1) == null) { // -1
                if (ast.getType() != HiveParser.KW_NOT) {
                    bk1.setCondition(ast.getText() + bk1.getCondition());
                    bk1.setBaseExpr(ast.getText() + bk1.getBaseExpr());
                }
            } else {
                Block bk2 = getBlockIteral((ASTNode) ast.getChild(1));
                bk1.getColSet().addAll(bk2.getColSet());
                bk1.getBaseColSet().addAll(bk2.getBaseColSet());

                bk1.getAllColSet().addAll(bk2.getAllColSet());
                bk1.getAllTableSet().addAll(bk2.getAllTableSet());

                bk1.setCondition(bk1.getCondition() + " " + ast.getText() + " " + bk2.getCondition());
                bk1.setBaseExpr(bk1.getBaseExpr() + " " + ast.getText() + " " + bk2.getBaseExpr());
                bk1.getTableSet().addAll(bk2.getTableSet());
                bk1.getBaseTableSet().addAll(bk2.getBaseTableSet());
            }
            return bk1;
        } else if (ast.getType() == HiveParser.TOK_FUNCTIONDI) { // 处理各种函数
            Block col = getBlockIteral((ASTNode) ast.getChild(1));
            String condition = ast.getChild(0).getText();
            col.setCondition(condition + "(distinct " + col.getCondition() + ")");
            col.setBaseExpr(condition + "(distinct " + col.getBaseExpr() + ")");
            return col;
        } else if (ast.getType() == HiveParser.TOK_FUNCTION) {
            String fun = ast.getChild(0).getText().toLowerCase();
            Block col = ast.getChild(1) == null ? new Block() : getBlockIteral((ASTNode) ast.getChild(1));
            if (ast.getParent().getType() == HiveParser.KW_NOT) {
                col.setCondition(col.getCondition() + " not ");
                col.setBaseExpr(col.getBaseExpr() + " not ");
            }
            if ("when".equalsIgnoreCase(fun)) {
                col.setCondition(getWhenCondition(ast, 1));
                col.setBaseExpr(getWhenCondition(ast, 2));
                Set<Block> processChilds = processChilds(ast, 1);
                bkToBlock(col, processChilds);
                // col.getBaseColSet().addAll(bkToBaseCols(col, processChilds));
                return col;
            } else if ("in".equalsIgnoreCase(fun)) {
                col.setCondition(col.getCondition() + " in (" + blockCondToString(processChilds(ast, 2), 1) + ")");
                col.setBaseExpr(col.getBaseExpr() + " in (" + blockCondToString(processChilds(ast, 2), 2) + ")");
                return col;
            } else if ("tok_isnotnull".equalsIgnoreCase(fun) //isnull isnotnull
                    || "tok_isnull".equalsIgnoreCase(fun)) {
                String func = fun.toLowerCase().substring(4).replace("is", " is ").replace("null", " null ");
                col.setCondition(col.getCondition() + " " + func);
                col.setBaseExpr(col.getBaseExpr() + " " + func);
                return col;
            } else if ("between".equalsIgnoreCase(fun)) {
                col.setCondition(getBlockIteral((ASTNode) ast.getChild(2)).getCondition()
                        + " between " + getBlockIteral((ASTNode) ast.getChild(3)).getCondition()
                        + " and " + getBlockIteral((ASTNode) ast.getChild(4)).getCondition());

                col.setBaseExpr(getBlockIteral((ASTNode) ast.getChild(2)).getBaseExpr()
                        + " between " + getBlockIteral((ASTNode) ast.getChild(3)).getBaseExpr()
                        + " and " + getBlockIteral((ASTNode) ast.getChild(4)).getBaseExpr());
                return col;
            } else if (ast.getChildren().size() > 1 && ast.getChild(ast.getChildCount() - 1).getType() == HiveParser.TOK_WINDOWSPEC) { // 开窗函数
                Block aggrCol = new Block();
                if (ast.getChildCount() > 2) {
                    aggrCol = getBlockIteral((ASTNode) ast.getChild(1));
                    aggrCol.setCondition(fun + "( " + aggrCol.getCondition() + " ) over (");
                    aggrCol.setBaseExpr(fun + "( " + aggrCol.getBaseExpr() + " ) over (");
                } else {
                    aggrCol.setBaseExpr(fun + "() over (");
                    aggrCol.setCondition(fun + "() over (");
                }
                praseWindows((ASTNode) ast.getChild(ast.getChildCount() - 1), aggrCol);
                return aggrCol;
            }
            Set<Block> processChilds = processChilds(ast, 1);
            bkToBlock(col, processChilds);
            // col.getBaseColSet().addAll(bkToBaseCols(col, processChilds));

            col.setCondition(fun + "(" + blockCondToString(processChilds, 1) + ")");
            col.setBaseExpr(fun + "(" + blockCondToString(processChilds, 2) + ")");
            return col;
        } else if (ast.getType() == HiveParser.LSQUARE) { //map,array
            Block column = getBlockIteral((ASTNode) ast.getChild(0));
            Block key = getBlockIteral((ASTNode) ast.getChild(1));
            column.setCondition(column.getCondition() + "[" + key.getCondition() + "]");
            column.setBaseExpr(column.getBaseExpr() + "[" + key.getBaseExpr() + "]");
            return column;
        } else {
            return parseBlock(ast);
        }
    }

    /**
     * @param ast
     * @return Block
     * @description 解析开窗函数
     * @author huyingtai
     * @date 2021/10/18 5:49 下午
     **/
    public void praseWindows(ASTNode ast, Block up) {
        switch ((ast.getToken().getType())) {
            case HiveParser.TOK_PARTITIONINGSPEC:
                for (int i = 0; i < ast.getChildCount(); i++) {
                    praseWindows((ASTNode) ast.getChild(i), up);
                }
                break;
            case HiveParser.TOK_DISTRIBUTEBY:
                Block partBk = getBlockIteral((ASTNode) ast.getChild(0));

                up.getColSet().addAll(partBk.getColSet());
                up.getBaseColSet().addAll(partBk.getBaseColSet());

                up.getAllColSet().addAll(partBk.getAllColSet());
                up.getAllTableSet().addAll(partBk.getAllTableSet());

                up.getTableSet().addAll(partBk.getTableSet());
                up.getBaseTableSet().addAll(partBk.getBaseTableSet());

                up.setBaseExpr(up.getBaseExpr() + " partition by  " + partBk.getBaseExpr());
                up.setCondition(up.getCondition() + " partition by " + partBk.getCondition());

                break;
            case HiveParser.TOK_ORDERBY:
                String rank = "asc";
                if (ast.getChild(0).getType() == HiveParser.TOK_TABSORTCOLNAMEDESC) {
                    rank = "desc";
                }
                Block orderBk = getBlockIteral((ASTNode) ast.getChild(0).getChild(0));

                up.getColSet().addAll(orderBk.getColSet());
                up.getBaseColSet().addAll(orderBk.getBaseColSet());

                up.getAllColSet().addAll(orderBk.getAllColSet());
                up.getAllTableSet().addAll(orderBk.getAllTableSet());

                up.getTableSet().addAll(orderBk.getTableSet());
                up.getBaseTableSet().addAll(orderBk.getBaseTableSet());

                up.setBaseExpr(up.getBaseExpr() + " order by " + orderBk.getBaseExpr() + " " + rank + ")");
                up.setCondition(up.getCondition() + " order by " + orderBk.getCondition() + " " + rank + ")");
                break;
            default:
                if (ast.getChildCount() > 0) {
                    praseWindows((ASTNode) ast.getChild(ast.getChildCount() - 1), up);
                }
        }


    }

    /**
     * 解析group条件
     */
    public Group getGroupByCondition(ASTNode ast) {
        Group group = new Group();
        group.setGroupExpr("");
        int cnt = ast.getChildCount();
        for (int i = 0; i < cnt; i++) {
            Block blockTmp = getBlockIteral((ASTNode) ast.getChild(i));
            ParseUtil.BlockToColumn(blockTmp, group);
            group.setGroupExpr(group.getGroupExpr() + "," + blockTmp.getBaseExpr());
        }
        group.setGroupExpr(group.getGroupExpr().substring(1));
        return group;
    }


    /**
     * 解析when条件
     *
     * @param ast
     * @return case when c1>100 then col1 when c1>0 col2 else col3 end
     */
    public String getWhenCondition(ASTNode ast, Integer type) {
        int cnt = ast.getChildCount();
        StringBuilder sb = new StringBuilder();
        String condition;
        for (int i = 1; i < cnt; i++) {
            if (type == 1) {
                condition = getBlockIteral((ASTNode) ast.getChild(i)).getCondition();
            } else {
                condition = getBlockIteral((ASTNode) ast.getChild(i)).getBaseExpr();
            }
            if (i == 1) {
                sb.append(" case when " + condition);
            } else if (i == cnt - 1) { //else
                sb.append(" else " + condition + " end ");
            } else if (i % 2 == 0) { //then
                sb.append(" then " + condition);
            } else {
                sb.append(" when " + condition);
            }
        }
        return sb.toString();
    }


    /**
     * 解析获得列名或者字符数字等和条件
     *
     * @param ast
     * @param ast
     * @return
     */
    public Block parseBlock(ASTNode ast) {
        if (ast.getType() == HiveParser.DOT
                && ast.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChild(0).getChildCount() == 1
                && ast.getChild(1).getType() == HiveParser.Identifier) {
            String column = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(1).getText());
            String alia = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(0).getChild(0).getText());
            return getBlock(ast, column, alia);
        } else if (ast.getType() == HiveParser.TOK_TABLE_OR_COL
                && ast.getChildCount() == 1
                && ast.getChild(0).getType() == HiveParser.Identifier) {
            String column = ast.getChild(0).getText();
            return getBlock(ast, column, null);
        } else if (ast.getType() == HiveParser.Number
                || ast.getType() == HiveParser.StringLiteral
                || ast.getType() == HiveParser.Identifier) {
            Block bk = new Block();
            bk.setCondition(ast.getText());
            bk.setBaseExpr(ast.getText());
            return bk;
        }
        return new Block();
    }


    public void bkToBlock(Block col, Set<Block> processChilds) {
        for (Block colLine : processChilds) {
            if (Check.notEmpty(colLine.getColSet())) {
                col.getColSet().addAll(colLine.getColSet());
            }

            if (Check.notEmpty(colLine.getBaseColSet())) {
                col.getBaseColSet().addAll(colLine.getBaseColSet());
            }

            if (Check.notEmpty(colLine.getTableSet())) {
                col.getTableSet().addAll(colLine.getTableSet());
            }

            if (Check.notEmpty(colLine.getBaseTableSet())) {
                col.getBaseTableSet().addAll(colLine.getBaseTableSet());
            }

            if (Check.notEmpty(colLine.getAllColSet())) {
                col.getAllColSet().addAll(colLine.getAllColSet());
            }

            if (Check.notEmpty(colLine.getAllTableSet())) {
                col.getAllTableSet().addAll(colLine.getAllTableSet());
            }

        }

    }

    public Set<String> bkToCols(Block col, Set<Block> processChilds) {
        Set<String> set = new LinkedHashSet<String>(processChilds.size());
        for (Block colLine : processChilds) {
            if (Check.notEmpty(colLine.getColSet())) {
                set.addAll(colLine.getColSet());
            }
        }
        return set;
    }

    public Set<String> bkToBaseCols(Block col, Set<Block> processChilds) {
        Set<String> set = new LinkedHashSet<String>(processChilds.size());
        for (Block colLine : processChilds) {
            if (Check.notEmpty(colLine.getBaseColSet())) {
                set.addAll(colLine.getBaseColSet());
            }
        }
        return set;
    }


    public String blockCondToString(Set<Block> processChilds, int type) {
        StringBuilder sb = new StringBuilder();
        for (Block colLine : processChilds) {
            if (type == 1) {
                sb.append(colLine.getCondition()).append(Constants.SPLIT_COMMA);
            } else {
                sb.append(colLine.getBaseExpr()).append(Constants.SPLIT_COMMA);
            }
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    /**
     * 从指定索引位置开始解析子树
     *
     * @param ast
     * @param startIndex 开始索引
     * @return
     */
    public Set<Block> processChilds(ASTNode ast, int startIndex) {
        int cnt = ast.getChildCount();
        Set<Block> set = new LinkedHashSet<Block>();
        for (int i = startIndex; i < cnt; i++) {
            Block bk = getBlockIteral((ASTNode) ast.getChild(i));
            if (Check.notEmpty(bk.getCondition()) || Check.notEmpty(bk.getColSet())) {
                set.add(bk);
            }
        }
        return set;
    }


    /**
     * 根据列名和别名获得块信息
     *
     * @param column
     * @param alia
     * @return
     */
    public Block getBlock(ASTNode ast, String column, String alia) {
        String[] result = getTableAndAlia(alia);
        String tableArray = result[0];
        String _alia = result[1];

        // 子查询来的
        for (String string : _alia.split(Constants.SPLIT_AND)) { //迭代循环的时候查询
            Select qt = queryMap.get(string.toLowerCase());
            if (Check.notEmpty(column) && qt != null) {
                for (Column colLine : qt.getColumnList()) {
                    if (column.equalsIgnoreCase(colLine.getToNameParse())) { // col1 as col 看这个  col 来源
                        Block bk = new Block();

                        bk.setCondition(colLine.getColCondition());
                        bk.setBaseExpr(alia == null ? column : alia + Constants.SPLIT_DOT + column);

                        bk.setColSet(ParseUtil.cloneSet(colLine.getColSet()));
                        bk.setTableSet(ParseUtil.cloneSet(colLine.getTableSet()));

                        bk.setAllColSet(ParseUtil.cloneSet(colLine.getAllColSet()));
                        bk.setAllTableSet(ParseUtil.cloneSet(colLine.getAllTableSet()));

                        bk.getBaseColSet().add(qt.getCurrent() + Constants.SPLIT_DOT + column);
                        bk.getBaseTableSet().add(qt.getCurrent());

                        return bk;
                    }
                }
            }
        }

        String _realTable = tableArray;
        int cnt = 0; //匹配字段和元数据字段相同数目，如果有多个匹配，即此sql有二义性
        for (String tables : tableArray.split(Constants.SPLIT_AND)) { //初始化的时候查询数据库对应表
            String[] split = tables.split("\\.");
            if (split.length > 2) {
                throw new SQLParseException("parse table:" + tables);
            }
            List<String> colByTab = MetaCache.getInstance().getColumnByDBAndTable(tables);
            for (String col : colByTab) {
                if (column.equalsIgnoreCase(col)) {
                    _realTable = tables;
                    cnt++;
                }
            }
        }
        if (cnt > 1) { //二义性检查
            throw new SQLParseException("SQL is ambiguity, column: " + column + " tables:" + tableArray);
        }

        Block bk = new Block();

        // 不知道字段来源哪个表的 从最后一个取
        String[] _realTables = _realTable.split(Constants.SPLIT_AND);
        _realTable = _realTables[_realTables.length - 1];
        String[] _alias = _alia.split(Constants.SPLIT_AND);
        _alia = _alias[_alias.length - 1];

        bk.setCondition(_realTable + Constants.SPLIT_DOT + column);
        bk.setBaseExpr(alia == null ? column : alia + Constants.SPLIT_DOT + column);


        bk.getColSet().add(_realTable + Constants.SPLIT_DOT + column);
        bk.getTableSet().add(_realTable);

        bk.getAllColSet().add(_realTable + Constants.SPLIT_DOT + column);
        bk.getAllTableSet().add(_realTable);

        bk.getBaseColSet().add(alia == null ? _alia + Constants.SPLIT_DOT + column : alia + Constants.SPLIT_DOT + column);
        bk.getBaseTableSet().add(alia == null ? _alia : alia);

        return bk;
    }


    /**
     * 根据别名查询表明
     *
     * @param alia
     * @return
     */
    public String[] getTableAndAlia(String alia) {
        String _alia = Check.notEmpty(alia) ? alia :
                ParseUtil.collectionToString(queryMap.keySet(), Constants.SPLIT_AND, true);
        String[] result = {"", _alia};
        LinkedHashSet<String> tableSet = new LinkedHashSet<String>();
        if (Check.notEmpty(_alia)) {
            String[] split = _alia.split(Constants.SPLIT_AND);
            for (String string : split) {
                //别名又分单独起的别名 和 表名，即 select a.col,table_name.col from table_name a
                if (inputTables.contains(string) || inputTables.contains(fillDB(string))) {
                    tableSet.add(fillDB(string));
                } else if (queryMap.containsKey(string.toLowerCase())) {
                    if (queryMap.get(string.toLowerCase()).getCodeType().equals(CodeType.TABLE)) {
                        tableSet.addAll(queryMap.get(string.toLowerCase()).getTableSet());
                    }
                }
            }
            result[0] = ParseUtil.collectionToString(tableSet, Constants.SPLIT_AND, true);
            result[1] = _alia;
        }
        return result;
    }

    public String fillDB(String nowTable) {
        if (Check.isEmpty(nowTable)) {
            return nowTable;
        }
        StringBuilder sb = new StringBuilder();
        String[] tableArr = nowTable.split(Constants.SPLIT_AND); //fact.test&test2&test3
        for (String tables : tableArr) {
            String[] split = tables.split("\\" + Constants.SPLIT_DOT);
            if (split.length > 2) {
                throw new SQLParseException("parse table:" + nowTable);
            }
            String db = split.length == 2 ? split[0] : nowQueryDB.toString();
            String table = split.length == 2 ? split[1] : split[0];
            sb.append(db).append(Constants.SPLIT_DOT).append(table).append(Constants.SPLIT_AND);
        }
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

}
