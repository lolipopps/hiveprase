package com.diven.hive.blood.parse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.diven.hive.blood.api.HiveBloodEngine;
import com.diven.hive.blood.api.HiveBloodEngineImpl;
import com.diven.hive.blood.model.Base;
import com.diven.hive.blood.model.Column;
import com.diven.hive.blood.model.Select;

import java.util.Arrays;
import java.util.List;

public class main {
    public static void main(String[] args) throws Exception {
        HiveBloodEngineImpl bloodEngine = new HiveBloodEngineImpl();
        SqlGen sqlGen = new SqlGen();
        //解析sql
        String[] hqls = {
                "insert OVERWRITE table dwd.dwd_comm_raw_shop_info_da partition(pt='${-1d_pt}')\n" +
                        "select  row_number() over(partition by shop_code order by city_code desc) as rank\n" +
                        "       ,DENSE_RANK() OVER(PARTITIon BY shop_code order by pv desc)        as d_rank2\n" +
                        "       ,ROW_NUMBER() OVER(PARTITIon BY shop_code)                         as rn3\n" +
                        "       ,sum(pv) over(PARTITIon BY shop_code order by city_code)           as pv1\n" +
                        "       ,NTILE(2) OVER(PARTITIon BY shop_code order by city_code)          as ntile1\n" +
                        "from dw.dw_table_2\n" +
                        "where pt not in ('12312', '123') "


        };

        List<? extends Base> res = bloodEngine.parser(Arrays.asList(hqls));
//        Column colRes = bloodEngine.getColumnLine(Arrays.asList(hqls), 0);
        printJsonString(res);
//        printJsonString(colRes);
       //  sqlGen.genSql((List<Select>) res);

    }

    /**
     * 输出标准的json字符串
     *
     * @param obj
     */
    public static void printJsonString(Object obj) {
        String str = JSON.toJSONString(obj,
                SerializerFeature.WriteMapNullValue,
                SerializerFeature.WriteNullListAsEmpty,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.PrettyFormat);
        System.out.println(str);
    }

    public static String toJsonString(Object obj) {
        String str = JSON.toJSONString(obj,
                SerializerFeature.WriteMapNullValue,
                SerializerFeature.WriteNullListAsEmpty,
                SerializerFeature.DisableCircularReferenceDetect,
                SerializerFeature.PrettyFormat);
        return str;
    }

}
