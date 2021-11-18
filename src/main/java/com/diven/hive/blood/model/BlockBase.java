package com.diven.hive.blood.model;

import lombok.Data;

import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;

@Data
public class BlockBase extends Base{
	public int id;
	// 来源字段
	public Set<String> tableSet = new TreeSet<>();
	// 字段条件
	public Set<String> colSet = new TreeSet<String>();
	// 字段条件
	public Set<String> baseColSet = new TreeSet<String>();
	// 来源字段
	public Set<String> baseTableSet = new TreeSet<String>();

	public Set<String> allColSet = new TreeSet<>();

	public Set<String> allTableSet = new TreeSet<>();
}
