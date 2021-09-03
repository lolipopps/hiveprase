package com.diven.hive.blood.enums;

import com.diven.hive.blood.utils.Check;

public enum CodeType {
    /**
     * column job
     */
    TABLE(0, "BASE_TABLE"),
    /**
     * meta job
     */
    SUB_SELECT(1, "SUB_SELECT");

    private final int type;

    private final String name;

    CodeType(int type, String name) {
        this.type = type;
        this.name = name;
    }

    public static CodeType getByName(String name) {
        if (Check.isEmpty(name)) {
            throw new IllegalArgumentException("WordSegType name cannot be null or empty , just support IK or WORD   !!! ");
        }
        switch (name) {
            case "BASE_TABLE":
                return TABLE;
            case "SUB_SELECT":
                return SUB_SELECT;
            default:
                throw new RuntimeException("just support WORD or IK meta !!!");
        }
    }
}