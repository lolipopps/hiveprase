package com.diven.hive.blood.parse;

import com.diven.hive.blood.enums.CodeType;
import com.diven.hive.blood.enums.Constants;
import com.diven.hive.blood.enums.JoinType;
import com.diven.hive.blood.exception.SQLParseException;
import com.diven.hive.blood.exception.UnSupportedException;
import com.diven.hive.blood.model.*;
import com.diven.hive.blood.utils.Check;
import com.diven.hive.blood.utils.MetaCache;
import com.diven.hive.blood.utils.ParseUtil;
import com.diven.hive.ql.parse.ASTNode;
import com.diven.hive.ql.parse.BaseSemanticAnalyzer;
import com.diven.hive.ql.parse.HiveParser;
import com.diven.hive.ql.parse.ParseDriver;
import org.antlr.runtime.tree.Tree;

import java.util.*;
import java.util.Map.Entry;

import static com.diven.hive.blood.utils.HqlUtil.notNormalCol;

/**
 * @author huyingttai
 * @Description hive sql 解析深度优先遍历实现
 * @create 12:23 下午 2021/8/23
 */
public class HiveSqlBloodFigureParser20210907 {

    private final Map<String /*table*/, List<String/*column*/>> dbMap = new HashMap<String, List<String>>();
    private final List<Select> selectList = new ArrayList<Select>(); //子查询树形关系保存
    private final Stack<Set<String>> conditionsStack = new Stack<Set<String>>();
    private final Stack<List<Column>> colsStack = new Stack<List<Column>>(); // 列堆栈
    private final Map<String, List<Column>> resultQueryMap = new HashMap<String, List<Column>>(); // 查询map
    private final Set<String> conditions = new HashSet<String>(); //where or join 条件缓存


    private final List<Column> cols = new ArrayList<Column>(); //一个子查询内的列缓存
    private final List<Column> unionCols = new ArrayList<Column>(); //一个子查询内的列缓存


    private final Stack<String> tableNameStack = new Stack<String>(); // 表名进库
    private final Stack<Boolean> joinStack = new Stack<Boolean>(); // 是否存在 关联 队列
    private final Stack<ASTNode> joinOnStack = new Stack<ASTNode>();
    private final LinkedHashMap<String, Select> queryMap = new LinkedHashMap<String, Select>();
    private final HashMap<Integer, Where> whereMap = new HashMap<>();
    private final HashMap<Integer, Group> groupMap = new HashMap<>();
    private final HashMap<Integer, Join> joinMap = new HashMap<>();
    private final ArrayList<Join> joinList = new ArrayList<>();
    private ASTNode joinOn = null;
    private boolean joinClause = false;
    private final StringBuilder nowQueryDB = new StringBuilder("default"); //hive的默认库
    private boolean isCreateTable = false;
    private String currentSql;
    //结果
    private final List<SQLResult> resultList = new ArrayList<SQLResult>();
    private final List<Column> columns = new ArrayList<Column>();
    private final Set<String> outputTables = new HashSet<String>();
    private String currOutputTable;


    private final Set<String> inputTables = new HashSet<String>();

    private final BlockParser blockParser;

    public HiveSqlBloodFigureParser20210907() {
        blockParser = new BlockParser();
        blockParser.setInputTables(inputTables);
        blockParser.setQueryMap(queryMap);
        blockParser.setNowQueryDB(nowQueryDB);
    }

    private void parseIteral(ASTNode ast) {
        prepareToParseCurrentNodeAndChilds(ast);
        parseChildNodes(ast);
        parseCurrentNode(ast);
        endParseCurrentNode(ast);
    }

    /**
     * 清洗sql中的参数，防止解析sql时，应为参数而报错。
     *
     * @param sql 清洗的sql
     * @return 转换后的sql
     */
    private String convertSqlVariable(String sql) {
        if (sql != null) {
            //替换参数
            sql = sql.replaceAll(Constants.regex_replace_params, "1024");
            //替换排除列：select `(name|id|pwd)?+.+` from table
            sql = sql.replaceAll(Constants.regex_replace_exclude, "1024");
            //替换 “ ` ”
            sql = sql.trim().replace("`", "");
        }
        return sql;
    }

    /**
     * 解析当前节点
     *
     * @param ast
     * @return
     */
    private void parseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
                //  输入输出字段的处理
                case HiveParser.TOK_SELEXPR:
                    //解析需要插入的表
                    Tree tok_insert = ast.getParent().getParent();
                    Tree child = tok_insert.getChild(0).getChild(0);
                    String tName = BaseSemanticAnalyzer.getUnescapedName((ASTNode) child.getChild(0));
                    String destTable = Constants.TOK_TMP_FILE.equals(tName) ? Constants.TOK_TMP_FILE : blockParser.fillDB(tName);
                    //select a.*,* from t1 和 select * from (select c1 as a,c2 from t1) t 的情况
                    if (ast.getChild(0).getType() == HiveParser.TOK_ALLCOLREF) {
                        String tableOrAlias = "";
                        if (ast.getChild(0).getChild(0) != null) {
                            tableOrAlias = ast.getChild(0).getChild(0).getChild(0).getText();
                        }
                        String[] result = blockParser.getTableAndAlia(tableOrAlias);
                        String _alia = result[1];

                        boolean isSub = false;  //处理嵌套select * 的情况
                        if (Check.notEmpty(_alia)) {
                            for (String string : _alia.split(Constants.SPLIT_AND)) { //迭代循环的时候查询
                                Select qt = queryMap.get(string.toLowerCase());
                                if (null != qt) {
                                    List<Column> columnList = qt.getColumnList();
                                    if (Check.notEmpty(columnList)) {
                                        isSub = true;
                                        for (Column column : columnList) {
                                            cols.add(column);
                                        }
                                    }
                                }
                            }
                        }
                        if (!isSub) { //处理直接select * 的情况
                            String nowTable = result[0];
                            String[] tableArr = nowTable.split(Constants.SPLIT_AND); //fact.test&test2
                            for (String tables : tableArr) {
                                String[] split = tables.split("\\.");
                                if (split.length > 2) {
                                    throw new SQLParseException("parse table:" + nowTable);
                                }

                                List<String> colByTab = MetaCache.getInstance().getColumnByDBAndTable(tables);
                                Block tmp = new Block();

                                for (String column : colByTab) {
                                    Set<String> fromNameSet = new LinkedHashSet<String>();
                                    fromNameSet.add(tables + Constants.SPLIT_DOT + column);
                                    tmp.setColSet(fromNameSet);
                                    tmp.setBaseColSet(fromNameSet);
                                    Column cl = new Column(column, tables + Constants.SPLIT_DOT + column, column,
                                            tmp, destTable);
                                    cols.add(cl);
                                }
                            }
                        }
                    } else {
                        Block bk = blockParser.getBlockIteral((ASTNode) ast.getChild(0));
                        String toNameParse = getToNameParse(ast, bk);
                        Column cl = new Column(toNameParse, bk.getCondition(), bk.getBaseExpr(), bk, destTable);
                        cols.add(cl);
                    }
                    break;


                case HiveParser.TOK_CREATETABLE: //outputtable
                    isCreateTable = true;
                    String tableOut = blockParser.fillDB(BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0)));
                    currOutputTable = tableOut;
                    outputTables.add(tableOut);
                    break;


                case HiveParser.TOK_TAB:// 输出的表
                    String tableTab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                    String tableOut2 = blockParser.fillDB(tableTab);
                    currOutputTable = tableOut2;
                    outputTables.add(tableOut2);
                    break;

                case HiveParser.TOK_TABREF:// inputTable
                    ASTNode tabTree = (ASTNode) ast.getChild(0);
                    String tableInFull = blockParser.fillDB((tabTree.getChildCount() == 1) ?
                            BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                            : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                            + Constants.SPLIT_DOT + BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(1))
                    );
                    inputTables.add(tableInFull);
                    queryMap.clear();
                    String alia = null;
                    if (ast.getChild(1) != null) { //(TOK_TABREF (TOK_TABNAME detail usersequence_client) c)
                        if (ast.getChild(1).getType() == HiveParser.TOK_INSERT) {
                            alia = "asdasda";
                        } else {
                            alia = ast.getChild(1).getText().toLowerCase();
                        }
                        Select qt = new Select();
                        qt.setCurrent(alia);
                        qt.setCodeType(CodeType.TABLE);
                        qt.getTableSet().add(tableInFull);
                        qt.getBaseTableSet().add(tableInFull);
                        Select pTree = ParseUtil.getSubQueryParent(ast);
                        qt.setPid(pTree.getPid());
                        qt.setParent(pTree.getParent());
                        selectList.add(qt);
                        if (joinClause && ast.getParent() == joinOn) { // TOK_SUBQUERY join TOK_TABREF ,此处的TOK_SUBQUERY信息不应该清除
                            for (Select entry : selectList) { //当前的查询范围
                                if (qt.getParent().equals(entry.getParent())) {
                                    queryMap.put(entry.getCurrent(), entry);
                                }
                            }
                        } else {
                            queryMap.put(qt.getCurrent(), qt);
                        }
                    } else {
                        alia = tableInFull;
                        Select qt = new Select();
                        qt.setCurrent(alia);
                        qt.setCodeType(CodeType.TABLE);
                        qt.getTableSet().add(tableInFull);
                        qt.getBaseTableSet().add(tableInFull);
                        Select pTree = ParseUtil.getSubQueryParent(ast);
                        qt.setPid(pTree.getPid());
                        qt.setQid(ParseUtil.getQueryParentId(ast));
                        qt.setParent(pTree.getParent());
                        selectList.add(qt);
                        if (joinClause && ast.getParent() == joinOn) {
                            for (Select entry : selectList) {
                                if (qt.getParent().equals(entry.getParent())) {
                                    queryMap.put(entry.getCurrent(), entry);
                                }
                            }
                        } else {
                            queryMap.put(qt.getCurrent(), qt);
                            //此处检查查询 select app.t1.c1,t1.c1 from t1 的情况
                            queryMap.put(tableInFull.toLowerCase(), qt);
                        }
                    }
                    break;

                case HiveParser.TOK_QUERY:
                    Integer qid = ParseUtil.generateTreeId(ast);
                    Select selectTemp = null;
                    // 最后一层是 TOK_QUERY 还要生成一层 否则这一层处理where 等
                    if (ast.getParent().isNil()) {
                        Select qt = new Select();
                        qt.setCurrent(currOutputTable);
                        qt.setColSelect(true);
                        qt.setCodeType(CodeType.SUB_SELECT);
                        Select pTree = ParseUtil.getSubQueryParent(ast);
                        qt.setId(ParseUtil.generateTreeId(ast));
                        qt.setPid(pTree.getPid());
                        qt.setParent(pTree.getParent());
                        for (Column cc : cols) {
                            cc.setToTable(qt.getCurrent());
                        }
                        qt.setColumnList(generateColLineList(cols, conditions));
                        qt.setChildList(getSubQueryChilds(qt.getId()));
                        if (Check.notEmpty(qt.getChildList())) {
                            for (Select cqt : qt.getChildList()) {
                                qt.getTableSet().addAll(cqt.getTableSet());  // 来源基础底表
                                qt.getBaseTableSet().add(cqt.getCurrent());  // 来源临时表
                                selectList.remove(cqt);  // 移除子节点信息
                            }
                        }
                        selectList.add(qt);
                        cols.clear();
                        queryMap.clear();
                        for (Select _qt : selectList) {
                            if (qt.getParent().equals(_qt.getParent())) { //当前子查询才保存
                                queryMap.put(_qt.getCurrent(), _qt);
                            }
                        }
                    }
                    // 处理 where
                    Where whereTmp = whereMap.get(qid);

                    for (Entry<String, Select> entry : queryMap.entrySet()) {
                        selectTemp = entry.getValue();
                        if (selectTemp.getQid() == qid) {
                            selectTemp.setWhere(whereTmp);
                            selectTemp.setGroup(groupMap.get(qid));
                        }
                        if (whereTmp != null) {
                            for (Column col : selectTemp.getColumnList()) {
                                col.getBaseTableSet().addAll(whereTmp.getBaseTableSet());
                                col.getBaseColSet().addAll(whereTmp.getBaseColSet());

                                col.getAllColSet().addAll(whereTmp.getColSet());
                                col.getAllTableSet().addAll(whereTmp.getTableSet());
                            }
                        }
                    }

                    break;
                // 重点 子查询情况比较复杂 分为 ( )t1  select * from ... ( ) t1 where
                case HiveParser.TOK_SUBQUERY:
                    // 1 是具体的子查询  2 是别名
                    if (ast.getChildCount() == 2) {
                        String tableAlias = "";
                        if (ast.getChild(1).getType() != HiveParser.TOK_INSERT) {
                            tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(1).getText());
                        }
                        Select qt = new Select();
                        qt.setQid(ParseUtil.getQueryParentId(ast));
                        qt.setCodeType(CodeType.SUB_SELECT);
                        qt.setCurrent(tableAlias.toLowerCase());
                        Select pTree = ParseUtil.getSubQueryParent(ast);
                        qt.setId(ParseUtil.generateTreeId(ast));
                        qt.setPid(pTree.getPid());
                        qt.setParent(pTree.getParent());
                        qt.setChildList(getSubQueryChilds(qt.getId()));


                        for (Column cc : cols) {
                            cc.setToTable(qt.getCurrent());
                        }
                        qt.setColumnList(generateColLineList(cols, conditions));
                        if (Check.notEmpty(qt.getChildList())) {
                            for (Select cqt : qt.getChildList()) {
                                qt.getTableSet().addAll(cqt.getTableSet());  // 来源基础底表
                                qt.getBaseTableSet().add(cqt.getCurrent());  // 来源临时表
                                selectList.remove(cqt);  // 移除子节点信息
                            }
                        }
                        selectList.add(qt);
                        cols.clear();
                        queryMap.clear();
                        for (Select _qt : selectList) {
                            if (qt.getParent().equals(_qt.getParent())) { //当前子查询才保存
                                queryMap.put(_qt.getCurrent(), _qt);
                            }
                        }


                    }
                    break;

                case HiveParser.TOK_WHERE: //3、过滤条件的处理select类
                    Block whereBk = blockParser.getBlockIteral((ASTNode) ast.getChild(0));
                    Where where = new Where();
                    where.setId(ParseUtil.getQueryParentId(ast));
                    where.setWhereExpr(whereBk.getBaseExpr());
                    ParseUtil.BlockToColumn(whereBk, where);
                    whereMap.put(where.getId(), where);
                    break;

                case HiveParser.TOK_GROUPBY: //3、过滤条件的处理select类
                    Group group = blockParser.getGroupByCondition(ast);
                    groupMap.put(group.getId(), group);
                    break;

                default:
                    //1、过滤条件的处理join类
                    if (joinOn != null && joinOn.getTokenStartIndex() == ast.getTokenStartIndex() && joinOn.getTokenStopIndex() == ast.getTokenStopIndex()) {
                        ASTNode astCon = (ASTNode) ast.getChild(2);
                        Block joinBLock = blockParser.getBlockIteral(astCon);
                        Join join = new Join();
                        join.setId(ParseUtil.getSubQueryParentId(ast));
                        ParseUtil.BlockToColumn(joinBLock, join);
                        join.setJoinType(JoinType.getByType(ast.getText().substring(4)));
                        join.setJoinExpr(joinBLock.getBaseExpr());
                        joinList.add(join);
                        selectList.get(selectList.size() - 1).setJoin(join);
                        joinMap.put(join.getId(), join);
                        break;
                    }
            }
        }

    }


    /**
     * 查找当前节点的子子查询节点（索引）
     *
     * @param id
     */
    private List<Select> getSubQueryChilds(int id) {
        List<Select> list = new ArrayList<Select>();
        for (int i = 0; i < selectList.size(); i++) {
            Select qt = selectList.get(i);
            if (id == qt.getPid()) {
                list.add(qt);
            }
        }
        return list;
    }

    /**
     * 获得要解析的名称
     *
     * @param ast
     * @param bk
     * @return
     */
    private String getToNameParse(ASTNode ast, Block bk) {
        String alia = "";
        Tree child = ast.getChild(0);
        if (ast.getChild(1) != null) { //有别名 ip as alia
            alia = ast.getChild(1).getText();
        } else if (child.getType() == HiveParser.DOT //没有别名 a.ip
                && child.getChild(0).getType() == HiveParser.TOK_TABLE_OR_COL
                && child.getChild(0).getChildCount() == 1
                && child.getChild(1).getType() == HiveParser.Identifier) {
            alia = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(1).getText());
        } else if (child.getType() == HiveParser.TOK_TABLE_OR_COL //没有别名 ip
                && child.getChildCount() == 1
                && child.getChild(0).getType() == HiveParser.Identifier) {
            alia = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
        }
        return alia;
    }


    /**
     * 保存subQuery查询别名和字段信息
     *
     * @param sqlIndex
     * @param tableAlias
     */
    private void putResultQueryMap(int sqlIndex, String tableAlias) {
        List<Column> list = generateColLineList(cols, conditions);
        String key = sqlIndex == 0 ? tableAlias : tableAlias + sqlIndex; //没有重名的情况就不用标记
        resultQueryMap.put(key, list);
    }

    private List<Column> generateColLineList(List<Column> cols, Set<String> conditions) {
        List<Column> list = new ArrayList<Column>();
        for (Column entry : cols) {
            list.add(ParseUtil.cloneColLine(entry));
        }
        return list;
    }


    /**
     * 解析所有子节点
     *
     * @param ast
     * @return
     */
    private void parseChildNodes(ASTNode ast) {
        int numCh = ast.getChildCount();
        if (numCh > 0) {
            for (int num = 0; num < numCh; num++) {
                ASTNode child = (ASTNode) ast.getChild(num);
                parseIteral(child);
            }
        }
    }

    /**
     * 准备解析当前节点
     *
     * @param ast
     */
    private void prepareToParseCurrentNodeAndChilds(ASTNode ast) {
        if (ast.getToken() != null) {
            switch (ast.getToken().getType()) {
                case HiveParser.TOK_SWITCHDATABASE:
                    nowQueryDB.delete(1, nowQueryDB.length());
                    nowQueryDB.append(ast.getChild(0).getText());
                    break;
                case HiveParser.TOK_TRANSFORM:
                    throw new UnSupportedException("no support transform using clause");
                case HiveParser.TOK_RIGHTOUTERJOIN:
                case HiveParser.TOK_LEFTOUTERJOIN:
                case HiveParser.TOK_JOIN:
                case HiveParser.TOK_LEFTSEMIJOIN:
                case HiveParser.TOK_MAPJOIN:
                case HiveParser.TOK_FULLOUTERJOIN:
                case HiveParser.TOK_UNIQUEJOIN:
                    joinStack.push(joinClause);
                    joinClause = true;
                    joinOnStack.push(joinOn);
                    joinOn = ast;
                    break;
            }
        }
    }


    /**
     * 结束解析当前节点
     *
     * @param ast
     */
    private void endParseCurrentNode(ASTNode ast) {
        if (ast.getToken() != null) {
            Tree parent = ast.getParent();
            switch (ast.getToken().getType()) { //join 从句结束，跳出join
                case HiveParser.TOK_RIGHTOUTERJOIN:
                case HiveParser.TOK_LEFTOUTERJOIN:
                case HiveParser.TOK_JOIN:
                case HiveParser.TOK_LEFTSEMIJOIN:
                case HiveParser.TOK_MAPJOIN:
                case HiveParser.TOK_FULLOUTERJOIN:
                case HiveParser.TOK_UNIQUEJOIN:
                    // 出现关联之后 同时是子查询就不应该搜出来
                    for (Select select : selectList) {
                        if (select.getCodeType() == CodeType.SUB_SELECT) {
                            select.setColSelect(false);
                        }
                    }
                    joinClause = joinStack.pop();
                    joinOn = joinOnStack.pop();
                    break;
                case HiveParser.TOK_QUERY:
                    processUnionStack(ast, parent); //union的子节点
                    break;
                case HiveParser.TOK_INSERT:
                    if (ast.getChild(0).getType() == HiveParser.TOK_INSERT_INTO) {
                        break;
                    }
                    Integer qid = ParseUtil.getQueryParentId(ast);
                    Select select = selectList.get(selectList.size() - 1);
                    if (select.getQid() == qid && select.getColSelect()) {
                        List<Column> colsTemp = generateColLineList(cols, conditions);
                        for (Column col : colsTemp) {
                            col.setToTable(select.getParent());
                        }
                        select.setColumnList(colsTemp);
                    }
                case HiveParser.TOK_SELECT:
                    break;
                case HiveParser.TOK_UNION:  //合并union字段信息
                    mergeUnionCols();
                    processUnionStack(ast, parent); //union的子节点
                    break;


            }
        }
    }

    private void mergeUnionCols() {
        ParseUtil.validateUnion(cols);
        int size = cols.size();
        int colNum = size / 2;
        List<Column> list = new ArrayList<Column>(colNum);
        for (int i = 0; i < colNum; i++) { //合并字段
            Column col = cols.get(i);
            for (int j = i + colNum; j < size; j = j + colNum) {
                Column col2 = cols.get(j);
                list.add(col2);
                if (notNormalCol(col.getToNameParse()) && !notNormalCol(col2.getToNameParse())) {
                    col.setToNameParse(col2.getToNameParse());
                }
                col.getColSet().addAll(col2.getColSet());
                col.getAllColSet().addAll(col2.getAllColSet());
                col.getTableSet().addAll(col2.getTableSet());
                col.getAllTableSet().addAll(col2.getAllTableSet());

                col.getBaseColSet().addAll(col2.getBaseColSet());
                col.getBaseTableSet().addAll(col2.getBaseTableSet());


                col.setColCondition(col.getColCondition() + Constants.SPLIT_AND + col2.getColCondition());
                col.setBaseExpr(col.getBaseExpr() + Constants.SPLIT_AND + col2.getBaseExpr());
            }
        }
        cols.removeAll(list); //移除已经合并的数据
    }

    private void processUnionStack(ASTNode ast, Tree parent) {
        boolean isNeedAdd = parent.getType() == HiveParser.TOK_UNION;
        if (isNeedAdd) {
            if (parent.getChild(0) == ast && parent.getChild(1) != null) {//是第一节点)
                //压栈
                conditionsStack.push(ParseUtil.cloneSet(conditions));
                conditions.clear();
                colsStack.push(ParseUtil.cloneList(cols));
                cols.clear();
            } else {  //无弟节点(是第二节点)
                //出栈
                selectList.get(selectList.size() - 1).setUnion(true);

                if (!conditionsStack.isEmpty()) {
                    conditions.addAll(conditionsStack.pop());
                }
                if (!colsStack.isEmpty()) {
                    unionCols.addAll(0, colsStack.peek());
                    cols.addAll(0, colsStack.pop());

                }
            }
        }
    }

    private void parseAST(ASTNode ast) {
        parseIteral(ast);
    }

    public List<? extends Base> parse(String sqlAll) throws Exception {
        if (Check.isEmpty(sqlAll)) {
            return selectList;
        }
        startParseAll(); //清空最终结果集
        int i = 0; //当前是第几个sql
        for (String sql : sqlAll.split("(?<!\\\\);")) {
            ParseDriver pd = new ParseDriver();
            String trim = sql.toLowerCase().trim();
            if (trim.startsWith("set") || trim.startsWith("add") || Check.isEmpty(trim)) {
                continue;
            }
            ASTNode ast = pd.parse(convertSqlVariable(sql));
            System.out.println(ast.toStringTree());
            prepareParse();
            this.currentSql = sql;
            parseAST(ast);
            endParse(++i);
        }
        return selectList;
    }

    /**
     * 清空上次处理的结果
     */
    private void startParseAll() {
        resultList.clear();
    }

    private void prepareParse() {
        this.currentSql = null;
        isCreateTable = false;
        dbMap.clear();

        columns.clear();
        outputTables.clear();
        inputTables.clear();

        queryMap.clear();
        selectList.clear();

        conditionsStack.clear(); //where or join 条件缓存
        colsStack.clear(); //一个子查询内的列缓存

        resultQueryMap.clear();
        conditions.clear(); //where or join 条件缓存
        cols.clear(); //一个子查询内的列缓存

        tableNameStack.clear();
        joinStack.clear();
        joinOnStack.clear();

        joinClause = false;
        joinOn = null;
    }

    /**
     * 所有解析完毕之后的后期处理
     */
    private void endParse(int sqlIndex) {
        putResultQueryMap(sqlIndex, Constants.TOK_EOF);
        putDBMap();
        setColLineList();
    }

    /***
     * 	设置输出表的字段对应关系
     */
    private void setColLineList() {
        Map<String, List<Column>> map = new HashMap<String, List<Column>>();
        for (Entry<String, List<Column>> entry : resultQueryMap.entrySet()) {
            if (entry.getKey().startsWith(Constants.TOK_EOF)) {
                List<Column> value = entry.getValue();
                for (Column column : value) {
                    List<Column> list = map.get(column.getToTable());
                    if (Check.isEmpty(list)) {
                        list = new ArrayList<Column>();
                        map.put(column.getToTable(), list);
                    }
                    list.add(column);
                }
            }
        }

        for (Entry<String, List<Column>> entry : map.entrySet()) {
            String table = entry.getKey();
            List<Column> pList = entry.getValue();
            List<String> dList = dbMap.get(table);
            int metaSize = Check.isEmpty(dList) ? 0 : dList.size();
            for (int i = 0; i < pList.size(); i++) { //按顺序插入对应的字段
                Column clp = pList.get(i);
                String colName = null;
                if (i < metaSize) {
                    colName = table + Constants.SPLIT_DOT + dList.get(i);
                }
                if (isCreateTable && Constants.TOK_TMP_FILE.equals(table)) {
                    for (String string : outputTables) {
                        table = string;
                    }
                }
                Column column = ParseUtil.cloneColLine(clp);
                column.setToTable(table);
                columns.add(column);
            }
        }

        //获取结果
        if (Check.notEmpty(columns) || Check.notEmpty(inputTables) || Check.notEmpty(outputTables)) {
            SQLResult sr = new SQLResult();
            sr.setCurrentSql(this.currentSql);
            sr.setColumnList(ParseUtil.cloneList(columns));
            sr.setInputTables(ParseUtil.cloneSet(inputTables));
            sr.setOutputTables(ParseUtil.cloneSet(outputTables));
            resultList.add(sr);
        }
    }


    private void putDBMap() {
        for (String table : outputTables) {
            List<String> list = MetaCache.getInstance().getColumnByDBAndTable(table);
            dbMap.put(table, list);
        }
    }


}
