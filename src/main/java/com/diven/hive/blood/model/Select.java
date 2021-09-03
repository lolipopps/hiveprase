package com.diven.hive.blood.model;

import com.diven.hive.blood.enums.CodeType;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author huyingttai
 * @Description
 * @create 10:10 上午 2021/8/23
 */

@Data
public class Select extends Base {
    private static final long serialVersionUID = 2888682925451956205L;
    private int id; // 当前子查询节点id
    private int pid; // 父节点id
    private int qid; // query 的id
    private String current;
    private String parent; // 只需父节点的名字
    private CodeType codeType;
    private Boolean colSelect = false;
    private Join join;
    private Where where;
    private Group group;
    private boolean isUnion = false;
    private Set<String> tableSet = new HashSet<String>();
    private Set<String> baseTableSet = new HashSet<String>();
    private List<Select> childList = new ArrayList<Select>();
    private List<Column> columnList = new ArrayList<Column>();

}