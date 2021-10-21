package com.diven.hive.blood.enums;

import com.diven.hive.blood.utils.Check;

public enum JoinType {
    /**
     * column job
     */
    LEFT_JOIN("TOK_LEFTOUTERJOIN", " LEFT JOIN "),
    /**
     * meta job
     */
    RIGHT_JOIN("TOK_RIGHTOUTERJOIN", " RIGHT JOIN "),

    JOIN("TOK_FULLOUTERJOIN", "JOIN");

    private final String type;

    private final String name;

    JoinType(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public static JoinType getByType(String type) {
        if (Check.isEmpty(type)) {
            throw new IllegalArgumentException("WordSegType name cannot be null or empty , just support left right  or full     !!! ");
        }
        switch (type) {
            case "TOK_LEFTOUTERJOIN":
                return LEFT_JOIN;
            case "TOK_RIGHTOUTERJOIN":
                return RIGHT_JOIN;
            case "TOK_FULLOUTERJOIN":
            case "JOIN":
                return JOIN;
            default:
                return JOIN;
        }
    }

    public static JoinType getByName(String name) {
        if (Check.isEmpty(name)) {
            throw new IllegalArgumentException("WordSegType name cannot be null or empty , just  left right  or full  !!! ");
        }
        switch (name) {
            case " LEFT JOIN ":
                return LEFT_JOIN;
            case " RIGHT JOIN ":
                return RIGHT_JOIN;
            case " JOIN ":
                return JOIN;
            default:
                throw new RuntimeException("just support left right  or full meta !!!");
        }
    }
}
