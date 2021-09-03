package com.diven.hive.blood.parse;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.diven.hive.blood.api.HiveBloodEngine;
import com.diven.hive.blood.api.HiveBloodEngineImpl;
import com.diven.hive.blood.model.Base;
import com.diven.hive.blood.model.Select;
import java.util.Arrays;
import java.util.List;

public class main {
    public static void main(String[] args) throws Exception {
        HiveBloodEngine bloodEngine = new HiveBloodEngineImpl();
        SqlGen sqlGen = new SqlGen();
        //解析sql
        String[] hqls = {
                "insert into aaa \n" +
                        "select  cola1\n" +
                        "       ,colb2\n" +
                        "from\n" +
                        "(\n" +
                        "\tselect  cola1\n" +
                        "\t       ,colb2\n" +
                        "\t       ,case when colb2 is not null then colb2  else cola1 end as cola3\n" +
                        "\tfrom\n" +
                        "\t(\n" +
                        "\t\tselect  cola1\n" +
                        "\t\t       ,cola2\n" +
                        "\t\tfrom\n" +
                        "\t\t(\n" +
                        "\t\t\tselect  cola1\n" +
                        "\t\t\t       ,cola2\n" +
                        "\t\t\t       ,row_number() over(partition by cola1 order by cola2 desc) as rank\n" +
                        "\t\t\tfrom dw.table_a1\n" +
                        "\t\t\twhere cola2 is not null \n" +
                        "\t\t) t1\n" +
                        "\t\twhere rank = 1 \n" +
                        "\t\tunion all\n" +
                        "\t\tselect  cola1\n" +
                        "\t\t       ,cola2\n" +
                        "\t\tfrom dw.table_a2\n" +
                        "\t\twhere pt = '${-1d_pt}' \n" +
                        "\t) t1\n" +
                        "\tleft join\n" +
                        "\t(\n" +
                        "\t\tselect  colb1\n" +
                        "\t\t       ,colb2\n" +
                        "\t\tfrom dw.table_b\n" +
                        "\t\twhere cola1 is not null \n" +
                        "\t)t2\n" +
                        "\ton t1.cola1 = t2.colb1\n" +
                        ") t1\n" +
                        "where cola3 is not null",
        };

        List<? extends Base> res = bloodEngine.parser(Arrays.asList(hqls));
        printJsonString(res);
        sqlGen.genSql((List<Select>) res);

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
