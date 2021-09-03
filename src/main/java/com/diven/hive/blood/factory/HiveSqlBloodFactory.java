package com.diven.hive.blood.factory;

import java.util.Arrays;
import java.util.List;

import com.diven.hive.blood.api.HiveBloodEngine;
import com.diven.hive.blood.api.HiveBloodEngineImpl;
import com.diven.hive.blood.model.*;
import com.diven.hive.blood.model.Base;

/**
 * 血缘工具
 * 方便于应用程序直接调用
 * @author diven
 *
 */
public class HiveSqlBloodFactory {
	
	/**血缘引擎**/
	private final static HiveBloodEngine bloodEngine = new HiveBloodEngineImpl();
	
	/**
	 * 解析字段血缘
	 * @param hsql    sqls
	 * @return	血缘关系
	 * @throws Exception
	 */
	public static List<? extends Base> parser(String[] hsql) throws Exception{
		return bloodEngine.parser(Arrays.asList(hsql));
	}
	

}
