package com.diven.hive.blood.api;

import java.util.List;

import com.diven.common.hive.blood.model.*;
import com.diven.hive.blood.model.Base;


/**
 * 血缘接口
 * 方便与spring注入
 * @author diven
 */
public interface HiveBloodEngine {

	/**
	 * 根据sql获取解析结果
	 * @param hqls
	 * @return	hsql解析结果
	 * @throws Exception
	 */
	public List<? extends Base> parser(List<String> hqls) throws Exception;
	

	
}
