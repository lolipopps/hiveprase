package com.diven.hive.blood.model;

import lombok.Data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

@Data
public class ColumnBase extends Base{
	public int id;
	// 来源字段
	public Set<String> tableSet = new LinkedHashSet<String>();
	// 字段条件
	public Set<String> colSet = new LinkedHashSet<String>();
	// 字段条件
	public Set<String> baseColSet = new LinkedHashSet<String>();
	// 来源字段
	public Set<String> baseTableSet = new LinkedHashSet<String>();

	public Set<String> allColSet = new HashSet<>();

	public Set<String> allTableSet = new HashSet<>();
}
