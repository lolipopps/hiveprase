package com.diven.hive.blood.model;

import lombok.Data;

@Data
public class Table extends Base{
	
	private static final long serialVersionUID = -1040938868973335902L;
	private String dbName = "default";
	private String tableName;
	
	public Table(String tableName) {
		this.tableName = tableName;
	}

	public Table(String dbName, String tableName) {
		this.dbName = dbName;
		this.tableName = tableName;
	}

	
}
