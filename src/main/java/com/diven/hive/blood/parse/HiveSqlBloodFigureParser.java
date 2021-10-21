package com.diven.hive.blood.parse;

import java.util.*;
import java.util.Map.Entry;

import com.diven.hive.blood.enums.CodeType;
import com.diven.hive.blood.enums.JoinType;
import com.diven.hive.blood.model.*;
import com.diven.hive.blood.enums.Constants;
import com.diven.hive.blood.exception.SQLParseException;
import com.diven.hive.blood.exception.UnSupportedException;
import com.diven.hive.blood.utils.MetaCache;
import com.diven.hive.blood.utils.ParseUtil;
import com.diven.hive.ql.parse.ASTNode;
import com.diven.hive.ql.parse.HiveParser;
import com.diven.hive.ql.parse.ParseDriver;
import org.antlr.runtime.tree.Tree;
import com.diven.hive.blood.utils.Check;
import com.diven.hive.ql.parse.BaseSemanticAnalyzer;

import static com.diven.hive.blood.utils.ParseUtil.getQueryParent;

/**
 * @author huyingttai
 * @Description hive sql 解析深度优先遍历实现
 * @create 12:23 下午 2021/8/23
 */
public class HiveSqlBloodFigureParser {

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
    private final Stack<Join> joinCondiction = new Stack<Join>();
    private final LinkedHashMap<String, Select> queryMap = new LinkedHashMap<String, Select>(); // 存储每个嵌套的语句
    private final LinkedHashMap<String, Select> withQueryMap = new LinkedHashMap<String, Select>(); // 存储每个嵌套的语句
    private final Map<Integer, Select> queryMaps = new HashMap<Integer, Select>();

    private boolean withQuery = false;


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

    public HiveSqlBloodFigureParser() {
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
                case HiveParser.TOK_SELEXPR: {
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
                }

                case HiveParser.TOK_CREATETABLE: //create 语句
                    isCreateTable = true;
                    String tableOut = blockParser.fillDB(BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0)));
                    currOutputTable = tableOut;
                    outputTables.add(tableOut);
                    break;
                case HiveParser.TOK_TAB:// //insert语句
                    String tableTab = BaseSemanticAnalyzer.getUnescapedName((ASTNode) ast.getChild(0));
                    String tableOut2 = blockParser.fillDB(tableTab);
                    currOutputTable = tableOut2;
                    outputTables.add(tableOut2);
                    break;

                case HiveParser.TOK_TABREF:// from 表语句
                {
                    ASTNode tabTree = (ASTNode) ast.getChild(0);

                    boolean isOnlyTable = ast.getParent().getType() != HiveParser.TOK_FROM;

                    String tableInFull = blockParser.fillDB((tabTree.getChildCount() == 1) ?
                            BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                            : BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(0))
                            + Constants.SPLIT_DOT + BaseSemanticAnalyzer.getUnescapedName((ASTNode) tabTree.getChild(1))
                    );
                    // with 语法的
                    Select tmpSelect;
                    if (tableInFull.startsWith("default")) {
                        tmpSelect = withQueryMap.get(tableInFull.split("\\.")[1]);
                    } else {
                        tmpSelect = new Select();
                        tmpSelect.getTableSet().add(tableInFull);
                        tmpSelect.getBaseTableSet().add(tableInFull);
                    }


                    inputTables.addAll(tmpSelect.getBaseTableSet());
                    String alia = null;
                    if (ast.getChild(1) != null) { //(TOK_TABREF (TOK_TABNAME detail usersequence_client) c)
                        alia = ast.getChild(1).getText().toLowerCase();
                    } else {
                        alia = tableInFull;
                    }
                    Select qt = new Select();
                    qt.setCurrent(alia);
                    qt.setAlias(alia);
                    qt.setCodeType(CodeType.TABLE);
                    qt.getTableSet().addAll(tmpSelect.getTableSet());
                    qt.getBaseTableSet().addAll(tmpSelect.getBaseTableSet());
                    qt.setColumnList(tmpSelect.getColumnList());
                    if (!isOnlyTable) {
                        Select pq = ParseUtil.getQueryParent(ast);
                        qt.setQid(pq.getPid());
                        qt.setId(pq.getPid());
                        qt.setBeginId(pq.getBeginId());
                        qt.setEndId(pq.getEndId());
                    } else {
                        qt.setQid(ParseUtil.generateTreeId(ast));
                        qt.setId(ParseUtil.generateTreeId(ast));
                    }
                    Select pTree = ParseUtil.getSubQueryParent(ast);
                    qt.setPid(pTree.getId());
                    qt.setParent(pTree.getParent());
                    selectList.add(qt);
//                    queryMap.put(qt.getCurrent(), qt);

//                    if(withQuery){
//                        withQueryMap.put(qt.getCurrent(), qt);
//                    }

                    queryMaps.put(qt.getId(), qt);

                    break;
                }
                case HiveParser.TOK_QUERY: {
                    Integer qid = ParseUtil.generateTreeId(ast);
                    Select selectTmp;
                    // 从表来的查询
                    // 如果原始就有 复制就可以
                    if (queryMaps.containsKey(qid)) {
                        selectTmp = queryMaps.get(qid);
                    } else {
                        selectTmp = new Select();
                        selectTmp.setQid(qid);
                        selectTmp.setId(qid);
                        queryMaps.put(qid, selectTmp);
                        selectList.add(selectTmp);
                    }
                    selectTmp.setPid(ParseUtil.getSubQueryParentId(ast));
                    selectTmp.setColumnList(generateColLineList(cols, conditions));
                    selectTmp.setChildList(getQueryChilds(selectTmp.getId()));

                    selectTmp.setBeginId(ast.getTokenStartIndex());
                    selectTmp.setEndId(ast.getTokenStopIndex());

                    if (Check.notEmpty(selectTmp.getChildList())) {
                        for (Select cqt : selectTmp.getChildList()) {
                            selectTmp.getTableSet().addAll(cqt.getTableSet());  // 来源基础底表
                            selectTmp.getBaseTableSet().add(cqt.getCurrent());  // 来源临时表
                            selectList.remove(cqt);  // 移除子节点信息
                        }
                    }
                    Where whereTmp = whereMap.get(qid);


                    for (Map.Entry<Integer, Select> entry : queryMaps.entrySet()) {
                        selectTmp = entry.getValue();
                        if (selectTmp.getQid() == qid) {
                            selectTmp.setWhere(whereTmp);
                            selectTmp.setGroup(groupMap.get(qid));
                            if (whereTmp != null) {
                                for (Column col : selectTmp.getColumnList()) {
                                    col.getBaseTableSet().addAll(whereTmp.getBaseTableSet());
                                    col.getBaseColSet().addAll(whereTmp.getBaseColSet());
                                    col.getAllColSet().addAll(whereTmp.getColSet());
                                    col.getAllTableSet().addAll(whereTmp.getTableSet());
                                }
                            }

                        }
                    }
                    break;
                }
                // 重点 子查询情况比较复杂 分为 ( )t1  select * from ... ( ) t1 where
                case HiveParser.TOK_SUBQUERY: {
                    // 1 是具体的子查询  2 是别名
                    Select selectTmp;
                    if (ast.getChildCount() == 2) {
                        String tableAlias = "";
                        if (ast.getChild(1).getType() != HiveParser.TOK_INSERT) {
                            tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(ast.getChild(1).getText());
                        }
                        if (ast.getChild(0).getType() == HiveParser.TOK_QUERY) {
                            selectTmp = queryMaps.get(ParseUtil.generateTreeId(ast.getChild(0)));
                            selectTmp.setPid(ParseUtil.getQueryParentId(ast));
                        } else {
                            selectTmp = new Select();
                            // 父查询
                            if (selectList.size() == 1) {
                                selectTmp.setQid(ParseUtil.getQueryChildId(ast));
                                selectTmp.setId(ParseUtil.getQueryChildId(ast));
                            } else {
                                selectTmp.setQid(ParseUtil.generateTreeId(ast));
                                selectTmp.setId(ParseUtil.generateTreeId(ast));
                            }
                            if (ast.getChild(0).getType() == HiveParser.TOK_UNION) {
                                selectTmp.setColumnList(generateColLineList(unionCols, conditions));
                                unionCols.clear();
                            }
                            selectTmp.setChildList(getQueryChilds(selectTmp.getId()));
                            queryMaps.put(selectTmp.getId(), selectTmp);
                            selectList.add(selectTmp);
                        }
                        Select pTree = ParseUtil.getSubQueryParent(ast);
                        selectTmp.setParent(pTree.getParent());
                        selectTmp.setAlias(tableAlias.toLowerCase());
                        selectTmp.setCurrent(tableAlias.toLowerCase());
                        selectTmp.setCodeType(CodeType.SUB_SELECT);
                        selectTmp.setPid(pTree.getId());
                        selectTmp.setBeginId(ast.getTokenStartIndex());
                        selectTmp.setEndId(ast.getTokenStopIndex());
                        for (Column col : selectTmp.getColumnList()) {
                            col.setToTable(selectTmp.getAlias());
                        }
                        cols.clear();
//                        queryMap.clear();


                        if (Check.notEmpty(selectTmp.getChildList())) {
                            for (Select cqt : selectTmp.getChildList()) {
                                selectTmp.getTableSet().addAll(cqt.getTableSet());  // 来源基础底表
                                selectTmp.getBaseTableSet().add(cqt.getCurrent());  // 来源临时表
                                cqt.setParent(selectTmp.getAlias());
                                selectList.remove(cqt);  // 移除子节点信息
                            }
                        }


                        for (Select _qt : selectList) {
//                            if (selectTmp.getPid() != 0 && (_qt.getPid() == selectTmp.getId() || _qt.getQid() == selectTmp.getId())) { //当前子查询才保存
//                                queryMap.put(_qt.getAlias(), _qt);
                            if (withQuery) {
                                withQueryMap.put(_qt.getCurrent(), _qt);
                            }
//                            }
                        }
                    }
                    break;
                }
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
                    group.setId(ParseUtil.getSubQueryParentId(ast));
                    groupMap.put(group.getId(), group);
                    break;

                default:
                    //1、过滤条件的处理join类
                    if (joinOn != null) {
                        Join join = joinCondiction.peek();
                        join.setId(ParseUtil.getSubQueryParentId(ast));
                        joinList.add(join);
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
    private List<Select> getQueryChilds(int id) {
        List<Select> list = new ArrayList<Select>();
        for (int i = 0; i < selectList.size(); i++) {
            Select qt = selectList.get(i);
            if (id == qt.getPid() && qt.getId() != qt.getPid()) {
                list.add(qt);
            }
        }
        return list;
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
            LinkedList<ASTNode> list = new LinkedList<>();
            for (int num = 0; num < numCh; num++) {
                ASTNode child = (ASTNode) ast.getChild(num);
                if (child.getType() == HiveParser.TOK_CTE) { // with 语法要优先处理
                    list.addFirst(child);
                } else {
                    list.addLast(child);
                }
            }
            for (ASTNode li : list) {
                parseIteral(li);
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
                    Join join = new Join();
                    joinCondiction.push(join);
                    break;
                case HiveParser.TOK_INSERT:
                    // 处理子查询
                    queryMap.clear();
                    Integer qid = ParseUtil.getQueryParentId(ast);
                    for (Select select : selectList) {
                        if (select.getQid() == qid || select.getPid() == qid) {
                            queryMap.put(select.getAlias() == null ? select.getCurrent() : select.getAlias(), select);
                        }
                    }
                    break;
                case HiveParser.TOK_CTE:
                    withQuery = true;
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
                    Join join = joinCondiction.pop();
                    queryMap.clear();
                    Integer qid = ParseUtil.getQueryParentId(ast);
                    for (Select select : selectList) {
                        if (select.getQid() == qid || select.getPid() == qid) {
                            queryMap.put(select.getAlias() == null ? select.getCurrent() : select.getAlias(), select);
                        }
                    }
                    // 处理join
                    join = blockParser.getJoinCondiction(ast);
                    Iterator<Entry<String, Select>> it = queryMap.entrySet().iterator();
                    Entry<String, Select> tail = null;
                    while (it.hasNext()) {
                        tail = it.next();
                    }
                    if (tail != null) {
                        tail.getValue().setJoin(join);
                    }
                    break;
                case HiveParser.TOK_QUERY:
                    processUnionStack(ast, parent); //union的子节点
                    break;
                case HiveParser.TOK_INSERT:
                    if (ast.getChild(0).getType() == HiveParser.TOK_INSERT_INTO) {
                        break;
                    }
                    qid = ParseUtil.getQueryParentId(ast);
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
                    processUnionStack(ast, parent); //union的子节点
                    break;
                case HiveParser.TOK_CTE:
                    withQuery = false;
                    selectList.clear();
                    queryMap.clear();
                    queryMaps.clear();
                    break;

            }
        }
    }

    private void processUnionStack(ASTNode ast, Tree parent) {
        boolean isNeedAdd = parent.getType() == HiveParser.TOK_UNION;
        if (isNeedAdd) {
            Integer id = ParseUtil.generateTreeId(ast);
            // 获取 union 打上标记
            for (Select select : selectList) {
                if (select.getId() == id) {
                    select.setUnion(true);
                }
            }
            if (parent.getChild(0) == ast && parent.getChild(1) != null) {//是第一节点)
                //压栈
                unionCols.addAll(ParseUtil.deepCloneList(cols));

            } else {  //无弟节点(是第二节点)
                //出栈
                //出栈
                for (int i = 0; i < cols.size(); i++) {
                    Column allColumn = unionCols.get(i);
                    Column colColumn = cols.get(i);
                    allColumn.getColSet().addAll(colColumn.getColSet());
                    allColumn.getAllColSet().addAll(colColumn.getAllColSet());
                    allColumn.getTableSet().addAll(colColumn.getTableSet());
                    allColumn.getAllTableSet().addAll(colColumn.getAllTableSet());
                    allColumn.getBaseColSet().addAll(colColumn.getBaseColSet());
                    allColumn.getBaseTableSet().addAll(colColumn.getBaseTableSet());
                }
            }
            cols.clear();
            conditions.clear();
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
