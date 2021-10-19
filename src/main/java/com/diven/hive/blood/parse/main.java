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
                        "select  t0.shop_code \n" +
                        "       ,nvl(coalesce(t1.shop_name,t3.org_name),'')                     as shop_name \n" +
                        "       ,nvl(t1.business_type ,-911 )                                   as business_type \n" +
                        "       ,nvl(t1.ehr_status ,-911 )                                      as ehr_status \n" +
                        "       ,nvl(t1.physical_id ,-911 )                                     as physical_id \n" +
                        "       ,nvl(t1.shop_status ,-911 )                                     as shop_status \n" +
                        "       ,nvl(t1.shop_status_external ,-911 )                            as shop_status_external \n" +
                        "       ,nvl(t1.shop_id ,-911 )                                         as shop_id \n" +
                        "       ,nvl(t1.shop_type ,-911 )                                       as shop_type \n" +
                        "       ,nvl(t1.running_id ,-911 )                                      as running_id \n" +
                        "       ,nvl(t2.franchiser_id,-911)                                     as franchiser_id \n" +
                        "       ,nvl(t1.brand_type,-911)                                        as brand_type \n" +
                        "       ,case when t1.brand_type='1' then 990004001 \n" +
                        "             when t1.brand_type='2' then 990004002 \n" +
                        "             when t1.brand_type='3' then 990004005  else 990004009 end as brand_code \n" +
                        "       ,case when t1.brand_type='1' then '链家' \n" +
                        "             when t1.brand_type='2' then '德佑' \n" +
                        "             when t1.brand_type='3' then 'KA'  else '其他' end           as brand_name \n" +
                        "       ,nvl(t1.data_source ,-911 )                                     as data_source \n" +
                        "       ,nvl(t1.attribute_source ,-911 )                                as attribute_source ---组织关系信息 \n" +
                        "       ,nvl(coalesce(t1.city_code,t3.city_code),-911)                  as city_code \n" +
                        "       ,nvl(t3.corp_code ,'')                                          as corp_code \n" +
                        "       ,nvl(t3.corp_name ,'')                                          as corp_name \n" +
                        "       ,nvl(t3.func_code ,'')                                          as func_code \n" +
                        "       ,nvl(t3.func_name ,'')                                          as func_name \n" +
                        "       ,nvl(t3.region_code ,'')                                        as region_code \n" +
                        "       ,nvl(t3.region_name ,'')                                        as region_name \n" +
                        "       ,nvl(t3.marketing_code ,'')                                     as marketing_code \n" +
                        "       ,nvl(t3.marketing_name ,'')                                     as marketing_name \n" +
                        "       ,nvl(t3.area_code ,'')                                          as area_code \n" +
                        "       ,nvl(t3.area_name ,'')                                          as area_name \n" +
                        "       ,nvl(t3.alliance_code ,'')                                      as alliance_code \n" +
                        "       ,nvl(t3.alliance_name ,'')                                      as alliance_name \n" +
                        "       ,nvl(t1.district_id ,'')                                        as district_id \n" +
                        "       ,nvl(t1.district_code ,'')                                      as district_code \n" +
                        "       ,nvl(t1.district_name ,'')                                      as district_name \n" +
                        "       ,nvl(t1.latitude ,-911)                                         as latitude \n" +
                        "       ,nvl(t1.longitude ,-911)                                        as longitude \n" +
                        "       ,nvl(t1.shop_address ,'')                                       as shop_address \n" +
                        "       ,nvl(t1.is_real_shop ,-911)                                     as is_real_shop \n" +
                        "       ,nvl(t1.status ,-911)                                           as is_valid \n" +
                        "       ,nvl(t1.shadow ,-911)                                           as is_shadow \n" +
                        "       ,nvl(t2.shop_area ,-911)                                        as shop_area \n" +
                        "       ,nvl(t2.plaque_width ,-911)                                     as plaque_width \n" +
                        "       ,nvl(t2.sign_status ,-911)                                      as sign_status \n" +
                        "       ,nvl(t2.shop_manager_name ,'')                                  as shop_manager_name \n" +
                        "       ,nvl(t2.shop_manager_telephone1 ,'')                            as shop_manager_telephone1 \n" +
                        "       ,nvl(t5.operate_ucid,-911)                                      as main_ca_ucid --主CA人员ucid \n" +
                        "       ,nvl(t5.operate_name,'')                                        as main_ca_name --主CA人员姓名 \n" +
                        "       ,nvl(t5.major_ucid,-911)                                        as major_ca_ucid --主CA的总监ucid \n" +
                        "       ,nvl(t5.major_name,'')                                          as major_ca_name --主CA的总监姓名 \n" +
                        "       ,nvl(t6.operate_ucid,-911)                                      as operate_ucid --运营官ucid \n" +
                        "       ,nvl(t6.operate_name,'')                                        as operate_name --运营官姓名 \n" +
                        "       ,nvl(t6.major_ucid,-911)                                        as operate_major_ucid --运营官的总监ucid \n" +
                        "       ,nvl(t6.major_name,'')                                          as operate_major_name --运营官的总监姓名 \n" +
                        "       ,nvl(t3.expire_date,'1000-01-01')                               as expire_date --解约日期 \n" +
                        "       ,nvl(t1.merge_time ,'1000-01-01 00:00:00' )                     as merge_time \n" +
                        "       ,nvl(t1.open_a_system_time ,'1000-01-01 00:00:00')              as open_a_system_time \n" +
                        "       ,nvl(t1.pre_typing_time ,'1000-01-01 00:00:00' )                as pre_typing_time \n" +
                        "       ,nvl(t1.connected_time ,'1000-01-01 00:00:00' )                 as connected_time \n" +
                        "       ,nvl(t3.org_id,-911)                                            as org_id \n" +
                        "       ,nvl(t11.shop_onwer_ucid,-911)                                  as shop_onwer_ucid \n" +
                        "       ,case when t3.org_code is not null and t1.shop_code is not null then 2 \n" +
                        "             when t3.org_code is not null then 0 \n" +
                        "             when t1.shop_code is not null then 1  else -911 end       as run_shop_type \n" +
                        "       ,substr(nvl(t1.connected_time ,'1000-01-01 00:00:00' ) ,1,10)   as link_date \n" +
                        "       ,substr(nvl(t1.connected_time ,'1000-01-01 00:00:00' ) ,1,7)    as link_month \n" +
                        "       ,case when connected_time is null or connected_time = '1000-01-01 00:00:00' then 0  else datediff('${-1d_yyyy-MM-dd}',substr(nvl(t1.connected_time ,'1000-01-01 00:00:00' ) ,1,10)) end as link_days \n" +
                        "       ,case when connected_time is null or connected_time = '1000-01-01 00:00:00' then 0  else months_between(concat(substr('${-1d_yyyy-MM-dd}',1,4),'-',substr('${-1d_yyyy-MM-dd}',6,2),'-01') ,concat(substr(to_date(nvl(t1.connected_time ,'1000-01-01 00:00:00' ) ),1,4) ,'-',substr(to_date(nvl(t1.connected_time ,'1000-01-01 00:00:00' ) ),6,2),'-01')) end as link_months \n" +
                        "       ,nvl(t12.org_code,'')                                           as franchiser_org_code \n" +
                        "       ,nvl(t1.corp_code,'')                                           as run_corp_code \n" +
                        "       ,nvl(t1.corp_name,'')                                           as run_corp_name \n" +
                        "       ,nvl(cooperate_status ,-911)                                    as cooperate_status_code \n" +
                        "       ,nvl(invalid_status ,-911)                                      as invalid_status_code \n" +
                        "       ,nvl(invalid_attachments,'')                                    as invalid_attachments\n" +
                        "       ,nvl(t13.state,-911)                                            as org_status_code\n" +
                        "from \n" +
                        "( ----取到所有的门店--- ---CA工作 台\n" +
                        "\tselect  distinct shop_code\n" +
                        "\tfrom dw.dw_hr_franchiser_running_shop_da\n" +
                        "\twhere pt= '${-1d_pt}' \n" +
                        "\tand city_code not in ('110000', '310000') union ALL \n" +
                        "\tselect  distinct org_code as shop_code\n" +
                        "\tfrom dw.dw_hr_org_da\n" +
                        "\twhere pt='${-1d_pt}' \n" +
                        "\tand org_type_id=14  \n" +
                        ")t0\n" +
                        "left join \n" +
                        "( --运营门店主表 数据来自CA平 台\n" +
                        "\tselect  shop_code \n" +
                        "\t       ,running_id \n" +
                        "\t       ,data_source \n" +
                        "\t       ,city_code \n" +
                        "\t       ,create_time \n" +
                        "\t       ,update_time \n" +
                        "\t       ,district_id \n" +
                        "\t       ,district_code \n" +
                        "\t       ,district_name \n" +
                        "\t       ,company_code as corp_code \n" +
                        "\t       ,company_name as corp_name ---存在和hr_org表不相等的情况 \n" +
                        "\t       ,shop_address \n" +
                        "\t       ,latitude \n" +
                        "\t       ,longitude \n" +
                        "\t       ,pos_num \n" +
                        "\t       ,business_license \n" +
                        "\t       ,is_real_shop \n" +
                        "\t       ,shop_status \n" +
                        "\t       ,status \n" +
                        "\t       ,shadow \n" +
                        "\t       ,shop_name \n" +
                        "\t       ,ehr_status \n" +
                        "\t       ,attribute_source \n" +
                        "\t       ,business_type \n" +
                        "\t       ,physical_id \n" +
                        "\t       ,brand_type \n" +
                        "\t       ,shop_id \n" +
                        "\t       ,shop_type \n" +
                        "\t       ,operate_time \n" +
                        "\t       ,connected_time \n" +
                        "\t       ,first_cdel_time \n" +
                        "\t       ,pre_typing_time \n" +
                        "\t       ,open_a_system_time \n" +
                        "\t       ,merge_time \n" +
                        "\t       ,shop_status_external \n" +
                        "\t       ,cooperate_status   \n" +
                        "\t       ,invalid_status   \n" +
                        "\t       ,invalid_attachments  \n" +
                        "\tfrom dw.dw_hr_franchiser_running_shop_da\n" +
                        "\twhere pt = '${-1d_pt}'  \n" +
                        ")t1\n" +
                        "on t0.shop_code=t1.shop_code\n" +
                        "left join \n" +
                        "( --门店基础信息表，对于加盟门店来说是拓店阶段记录的数 据\n" +
                        "\tselect  shop_id \n" +
                        "\t       ,franchiser_id \n" +
                        "\t       ,shop_area \n" +
                        "\t       ,plaque_width \n" +
                        "\t       ,open_shop_time \n" +
                        "\t       ,shop_manager_name \n" +
                        "\t       ,shop_manager_telephone1 \n" +
                        "\t       ,shop_manager_telephone1_md5 \n" +
                        "\t       ,old_sign_status as sign_status \n" +
                        "\t       ,sign_time \n" +
                        "\t       ,revoke_time \n" +
                        "\t       ,register_address\n" +
                        "\tfrom dw.dw_hr_franchiser_shop_da\n" +
                        "\twhere pt = '${-1d_pt}' \n" +
                        "\tand shop_id > 0 \n" +
                        "\tand status_code <> 170032004 --已删除数据  \n" +
                        ")t2\n" +
                        "on t2.shop_id = t1.shop_id\n" +
                        "left join \n" +
                        "(\n" +
                        "\tselect  org_code \n" +
                        "\t       ,city_code \n" +
                        "\t       ,org_name \n" +
                        "\t       ,org_level6_code as area_code \n" +
                        "\t       ,org_level6_name as area_name \n" +
                        "\t       ,org_level5_code as marketing_code \n" +
                        "\t       ,org_level5_name as marketing_name \n" +
                        "\t       ,org_level4_code as region_code \n" +
                        "\t       ,org_level4_name as region_name \n" +
                        "\t       ,org_level3_code as func_code \n" +
                        "\t       ,org_level3_name as func_name \n" +
                        "\t       ,corp_code \n" +
                        "\t       ,corp_name \n" +
                        "\t       ,alliance_code \n" +
                        "\t       ,alliance_name \n" +
                        "\t       ,mdm_code \n" +
                        "\t       ,expire_date \n" +
                        "\t       ,org_id\n" +
                        "\tfrom dw.dw_hr_org_da\n" +
                        "\twhere pt = '${-1d_pt}' \n" +
                        "\tand org_type_id=14  \n" +
                        ")t3\n" +
                        "on t0.shop_code = t3.org_code\n" +
                        "left join \n" +
                        "( --主CA人 员\n" +
                        "\tselect  running_id \n" +
                        "\t       ,operate_ucid \n" +
                        "\t       ,operate_name \n" +
                        "\t       ,major_ucid \n" +
                        "\t       ,major_name\n" +
                        "\tfrom \n" +
                        "\t(\n" +
                        "\t\tselect  running_id \n" +
                        "\t\t       ,operate_ucid \n" +
                        "\t\t       ,operate_name \n" +
                        "\t\t       ,major_ucid \n" +
                        "\t\t       ,major_name \n" +
                        "\t\t       ,row_number() over(partition by running_id order by create_time desc) as rank\n" +
                        "\t\tfrom dw.dw_hr_franchiser_running_shop_ca_relation_da\n" +
                        "\t\twhere pt = '${-1d_pt}' \n" +
                        "\t\tand status = 1 \n" +
                        "\t\tand operator_category = 'CA' \n" +
                        "\t\tand operate_type = 'MAIN'  \n" +
                        "\t) t1\n" +
                        "\twhere rank = 1  \n" +
                        ")t5\n" +
                        "on t5.running_id = t1.running_id\n" +
                        "left join \n" +
                        "( --运营 官\n" +
                        "\tselect  running_id \n" +
                        "\t       ,operate_ucid \n" +
                        "\t       ,operate_name \n" +
                        "\t       ,major_ucid \n" +
                        "\t       ,major_name\n" +
                        "\tfrom \n" +
                        "\t(\n" +
                        "\t\tselect  running_id \n" +
                        "\t\t       ,operate_ucid \n" +
                        "\t\t       ,operate_name \n" +
                        "\t\t       ,major_ucid \n" +
                        "\t\t       ,major_name \n" +
                        "\t\t       ,row_number() over(partition by running_id order by create_time desc) as rank\n" +
                        "\t\tfrom dw.dw_hr_franchiser_running_shop_ca_relation_da\n" +
                        "\t\twhere pt = '${-1d_pt}' \n" +
                        "\t\tand status = 1 \n" +
                        "\t\tand operator_category = 'YUN_YING_GUAN'  \n" +
                        "\t) t1\n" +
                        "\twhere rank = 1  \n" +
                        ")t6\n" +
                        "on t6.running_id = t1.running_id\n" +
                        "left join \n" +
                        "(\n" +
                        "\tselect  corp_code \n" +
                        "\t       ,brand_id \n" +
                        "\t       ,brand_name\n" +
                        "\tfrom dw.dw_hr_corp_da\n" +
                        "\twhere pt = '${-1d_pt}'  \n" +
                        ")t9\n" +
                        "on t9.corp_code = t3.corp_code\n" +
                        "left join \n" +
                        "(\n" +
                        "\tselect  org_code \n" +
                        "\t       ,expire_date\n" +
                        "\tfrom \n" +
                        "\t(\n" +
                        "\t\tselect  org_code \n" +
                        "\t\t       ,expire_date \n" +
                        "\t\t       ,row_number() over(partition by org_code order by create_time desc) as rank\n" +
                        "\t\tfrom dw.dw_hr_org_da\n" +
                        "\t\twhere pt='${-1d_pt}' \n" +
                        "\t\tand org_type_id=14  \n" +
                        "\t) t1\n" +
                        "\twhere rank = 1  \n" +
                        ")t10\n" +
                        "on t0.shop_code = t10.org_code\n" +
                        "left join \n" +
                        "(\n" +
                        "\tselect  org_code \n" +
                        "\t       ,owner_ucid as shop_onwer_ucid\n" +
                        "\tfrom \n" +
                        "\t(\n" +
                        "\t\tselect  org_code \n" +
                        "\t\t       ,owner_ucid \n" +
                        "\t\t       ,row_number() over(partition by org_code order by create_time desc) rn\n" +
                        "\t\tfrom dw.dw_hr_shop_owner_da\n" +
                        "\t\twhere pt='${-1d_pt}' \n" +
                        "\t\tand is_valid = 1  \n" +
                        "\t) t1\n" +
                        "\twhere rn = 1  \n" +
                        ") t11\n" +
                        "on t3.org_code = t11.org_code\n" +
                        "left join \n" +
                        "(\n" +
                        "\tselect  org_code \n" +
                        "\t       ,shop_id\n" +
                        "\tfrom \n" +
                        "\t(\n" +
                        "\t\tselect  shop_id \n" +
                        "\t\t       ,org_code \n" +
                        "\t\t       ,row_number() over(partition by shop_id order by update_time desc) rn\n" +
                        "\t\tfrom dw.dw_hr_franchiser_shop_org_mapping_da\n" +
                        "\t\twhere pt='${-1d_pt}'  \n" +
                        "\t) t1\n" +
                        "\twhere rn = 1  \n" +
                        ")t12\n" +
                        "on t1.shop_id = t12.shop_id\n" +
                        "left join\n" +
                        "(\n" +
                        "\tselect  org_code\n" +
                        "\t       ,ca_store_id\n" +
                        "\t       ,state\n" +
                        "\tfrom dw.dw_plat_personnel_org_da\n" +
                        "\twhere pt='${-1d_pt}'\n" +
                        "\tand org_type_id=14 --组织类型为门店\n" +
                        "\tand ca_store_id<>-911 --已关联实体\n" +
                        "\tand ca_store_id<>0 \n" +
                        ")t13\n" +
                        "on t1.shop_code = t13.org_code"


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
