package com.diven.hive.blood.api;

import java.util.List;

import com.diven.hive.blood.model.Column;
import com.diven.hive.blood.model.Select;
import com.diven.hive.blood.parse.HiveSqlBloodFigureParser;
import com.diven.hive.blood.utils.HqlUtil;
import com.diven.hive.blood.model.Base;

public class HiveBloodEngineImpl implements HiveBloodEngine {

    @Override
    public List<? extends Base> parser(List<String> hqls) throws Exception {
        return new HiveSqlBloodFigureParser().parse(HqlUtil.ListToString(hqls));
    }

    public Column getColumnLine(List<String> hqls, Integer index) throws Exception {
        List<? extends Base> res = new HiveSqlBloodFigureParser().parse(HqlUtil.ListToString(hqls));
        Select select = (Select) res.get(res.size() - 1);
        Column column = select.getColumnList().get(index);
        return column;
    }


}
