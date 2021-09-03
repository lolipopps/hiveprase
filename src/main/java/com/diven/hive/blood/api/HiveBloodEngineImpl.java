package com.diven.hive.blood.api;
import java.util.List;
import com.diven.hive.blood.parse.HiveSqlBloodFigureParser;
import com.diven.hive.blood.utils.HqlUtil;
import com.diven.hive.blood.model.Base;

public class HiveBloodEngineImpl implements HiveBloodEngine {
	
	@Override
	public List<? extends Base> parser(List<String> hqls) throws Exception {
		return new HiveSqlBloodFigureParser().parse(HqlUtil.ListToString(hqls));
	}
	

	
}
