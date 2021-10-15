package com.diven.hive.blood.model;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import lombok.Data;


/**
 * 生成的列的血缘关系
 *
 * @author divenwu
 */
@Data
public class Column extends ColumnBase {
    private static final long serialVersionUID = -1690060728674289074L;
    // 解析sql出来的列名称
    private String toNameParse;
    // 带条件的源字段
    private String colCondition;
    // 带条件的源字段
    private String baseExpr;
    private Set<String> allColSet = new HashSet<>();
    private Set<String> allTableSet = new HashSet<>();
    // 解析出来输出表
    private String toTable;
    private static final String CON_COLFUN = "";

    public Column() {
    }

    public Column(String toNameParse, String colCondition, String baseExpr, Block block, String toTable) {
        this.toNameParse = toNameParse;
        this.baseExpr = baseExpr;
        this.colCondition = colCondition;
        this.toTable = toTable;
        this.colSet.addAll(block.getColSet());
        this.allColSet.addAll(block.getAllColSet());
        this.baseColSet.addAll(block.getBaseColSet());
        this.tableSet.addAll(block.getTableSet());
        this.allTableSet.addAll(block.getAllTableSet());
        this.baseTableSet.addAll(block.getBaseTableSet());

    }
}