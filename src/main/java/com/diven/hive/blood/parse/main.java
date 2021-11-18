package com.diven.hive.blood.parse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
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
                        " select col2\n" +
                                "from dw.dwdwd\n" +
                                "union \n" +
                                "select col1\n" +
                                "from dw.dwdwc"


        };

        List<? extends Base> res = bloodEngine.parser(Arrays.asList(hqls));
//        Column colRes = bloodEngine.getColumnLine(Arrays.asList(hqls), 0);
        printJsonString(res);
//        printJsonString(colRes);
         System.out.println(sqlGen.genSql((List<Select>) res));

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
