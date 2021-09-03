package com.diven.hive.blood.model;

import lombok.Data;

@Data
public class ColumnNode extends Base{
	private static final long serialVersionUID = -8754340292102488397L;
	private long id;
	private String column;
	private long tableId;
	private String table;
	private String db;

}
