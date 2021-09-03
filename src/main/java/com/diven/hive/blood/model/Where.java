package com.diven.hive.blood.model;

import lombok.Data;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author huyingtai
 * @date 2021-07-06.
 */
@Data
public class Where extends ColumnBase {



    private String whereExpr;

}
