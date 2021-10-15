package com.diven.hive.blood.model;

import lombok.Data;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author huyingttai
 * @Description
 * @create 4:21 下午 2021/8/23
 */

@Data
public class Block extends Base {
    private static final long serialVersionUID = -4862449285786384062L;
    private String condition;
    private String baseExpr;
    private Set<String> colSet = new LinkedHashSet<String>();
    private Set<String> baseColSet = new LinkedHashSet<String>();
    private Set<String> tableSet = new LinkedHashSet<String>();
    private Set<String> baseTableSet = new LinkedHashSet<String>();
    private Set<String> allColSet = new HashSet<>();
    private Set<String> allTableSet = new HashSet<>();
}