package com.diven.hive.blood.model;

import com.diven.hive.blood.enums.JoinType;
import lombok.Data;

@Data
public class Join extends BlockBase {

    private String joinExpr;
    private JoinType joinType;

}
