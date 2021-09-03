package com.diven.hive.blood.enums;

import com.diven.hive.blood.utils.Check;

public enum JoinType {
    /**
     * column job
     */
    LEFT_JOIN("LEFTOUTERJOIN", " LEFT JOIN "),
    /**
     * meta job
     */
    RIGHT_JOIN("RIGHTOUTERJOIN", " RIGHT JOIN "),

    JOIN("FULLOUTERJOIN", "JOIN");

    private final String type;

    private final String name;

    JoinType(String type, String name) {
        this.type = type;
        this.name = name;
    }

    public static JoinType getByType(String type) {
        if (Check.isEmpty(type)) {
            throw new IllegalArgumentException("WordSegType name cannot be null or empty , just support IK or WORD   !!! ");
        }
        switch (type) {
            case "LEFTOUTERJOIN":
                return LEFT_JOIN;
            case "RIGHTOUTERJOIN":
                return RIGHT_JOIN;
            case "FULLOUTERJOIN":
            case "JOIN":
                return JOIN;
            default:
                throw new RuntimeException("just support WORD or IK meta !!!");
        }
    }

    public static JoinType getByName(String name) {
        if (Check.isEmpty(name)) {
            throw new IllegalArgumentException("WordSegType name cannot be null or empty , just support IK or WORD   !!! ");
        }
        switch (name) {
            case " LEFT JOIN ":
                return LEFT_JOIN;
            case " RIGHT JOIN ":
                return RIGHT_JOIN;
            case " JOIN ":
                return JOIN;
            default:
                throw new RuntimeException("just support WORD or IK meta !!!");
        }
    }
}
