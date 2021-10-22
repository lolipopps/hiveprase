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
                "insert OVERWRITE table rpt.rpt_plat_task_link_detail_info_da partition (pt='${-1d_pt}')\n" +
                        "select  task_link                as task_link_code\n" +
                        "       ,task_id                  as task_id\n" +
                        "       ,instr(task_link,task_id) as task_index\n" +
                        "       ,task_link_length         as task_link_length\n" +
                        "from\n" +
                        "(\n" +
                        "\tselect  task_link\n" +
                        "\t       ,split(task_link,'->')[0]                             as begin_task_id\n" +
                        "\t       ,split(task_link,'->')[size(split(task_link,'->'))-2] as end_task_id\n" +
                        "\t       ,size(split(task_link,'->'))-1                        as task_link_length\n" +
                        "\tfrom rpt.rpt_plat_task_link_info_da\n" +
                        "\twhere pt = '${-1d_pt}' \n" +
                        ") t1 LATERAL VIEW explode(split(task_link, '->')) num as task_id\n" +
                        "where task_id <> ''"


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
