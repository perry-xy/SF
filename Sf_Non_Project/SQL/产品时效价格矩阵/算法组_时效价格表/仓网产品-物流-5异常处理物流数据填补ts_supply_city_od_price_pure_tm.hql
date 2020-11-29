--标准表建立
create Table if not exists dm_as.ts_supply_city_od_error_flow_tm 
(product_code string comment '产品代码'
    ,product_name string comment '产品名称'
    ,src_code string comment '出发城市编码'
    ,src_city string comment '出发城市'
    ,dest_code string comment '目的城市编码'
    ,dest_city string comment '目的城市'
    ,error_type string comment '异常类型'
)comment '流向问题异常数据'
partitioned by (inc_month string);

create Table if not exists dm_as.ts_supply_city_od_price_error_flow_replace_tm 
(
    product_code string comment '产品代码'
    ,product_name string comment '产品名称'
    ,src_code string comment '出发城市编码'
    ,src_city string comment '出发城市'
    ,dest_code string comment '目的城市编码'
    ,dest_city string comment '目的城市'
    ,error_type string comment '异常类型'
    ,new_src_city string comment '替换的出发城市'
    ,avg_od_tm double comment '平均时效hour'
    ,od_tm_05 double comment '最快时效hour'
    ,od_tm_mid double comment '中位数时效hour'
    ,od_tm_95 double comment '最慢时效hour'
    ,od_tm_mode double comment '众数时效hour'
    ,od_tm_25 double comment '25百分位时效'
    ,od_tm_75 double comment '75百分位时效'
    ,intm_24_rt double comment '24小时达成率'
    ,intm_48_rt double comment '48小时达成率'
    ,intm_72_rt double comment '72小时达成率'
)comment '异常流向替代时效表'
partitioned by (inc_month string);

create Table if not exists dm_as.ts_supply_city_od_price_pure_tm 
(
    src_code string comment '原寄地代码'
    ,src_city string comment '原寄地城市'
    ,src_province string comment '原寄省'
    ,src_dist string comment '原寄区'
    ,src_area string comment '原寄大区'
    ,dest_code string comment '目的城市编码'
    ,dest_city string comment '目的城市'
    ,dest_province string comment '目的省'
    ,dest_dist string comment '目的区'
    ,dest_area string comment '目的大区'
    ,distance string comment '距离km'
    ,distance_section string comment '距离分段'
    ,product_code string comment '产品代码'
    ,product_name string comment '产品名称'
    ,add_mark int comment '是否补充数据，1是，0否，2经过异常处理'
    ,avg_od_tm double comment '平均时效hour'
    ,od_tm_05 double comment '最快时效hour'
    ,od_tm_mid double comment '中位数时效hour'
    ,od_tm_95 double comment '最慢时效hour'
    ,od_tm_mode double comment '众数时效hour'
    ,pay_country string comment '付款地国家'
    ,price_code string comment '价格区码'
    ,base_weight_qty double comment '首重'
    ,base_price double comment '首重运费'
    ,weight_price_qty double comment '续重运费'
    ,max_weight_qty double comment '最大重量'
    ,light_factor double comment '轻抛'
    ,od_normal int comment '该流向是否有对应产品价格信息'
    ,od_tm_25 double comment '25百分位时效'
    ,od_tm_75 double comment '75百分位时效'
    ,intm_24_rt double comment '24小时达成率'
    ,intm_48_rt double comment '48小时达成率'
    ,intm_72_rt double comment '72小时达成率'
    ,wh_avg_od_tm double comment '顺丰仓发出-平均时效hour'
    ,wh_od_tm_05 double comment '顺丰仓发出-最快时效'
    ,wh_od_tm_mid double comment '顺丰仓发出-中位数时效hour'
    ,wh_od_tm_95 double comment '顺丰仓发出-最慢时效hour'
    ,wh_od_tm_mode double comment '顺丰仓发出-众数时效hour'
    ,wh_od_tm_25 double comment '顺丰仓发出-25百分位时效'
    ,wh_od_tm_75 double comment '顺丰仓发出-75百分位时效'
    ,wh_intm_24_rt double comment '顺丰仓发出-24小时达成率'
    ,wh_intm_48_rt double comment '顺丰仓发出-48小时达成率'
    ,wh_intm_72_rt double comment '顺丰仓发出-72小时达成率'
)comment '清洗后的流向时效表'
partitioned by (inc_month string);

--获取异常流向
--跨城运输服务，且时长小于0.1的，认为是异常
alter table dm_as.ts_supply_city_od_error_flow_tm DROP IF EXISTS PARTITION (inc_month='$[month(yyyyMM,-1)]');
insert into table dm_as.ts_supply_city_od_error_flow_tm partition (inc_month = '$[month(yyyyMM,-1)]')
select product_code
      ,product_name
      ,src_code
      ,src_city
      ,dest_code
      ,dest_city
      ,'时效中位数太小' error_type
  from dm_as.ts_supply_org_city_name_od_tm a
 where inc_month = '$[month(yyyyMM,-1)]'
   and distance_section not like '%同城%'
   and od_tm_mid<=0.1;
        
--时长相比同省临近3个城市，比周边小50%，且不是二线以上的城市，则认为是异常
----其中：针对多个城市属于同一个编码，如果某城市归属于二线以上的城市，则认为这个城市也是二线以上的
--时长相比同省临近3个城市，比周边大50%，且是系统补足数据，则识别为异常
--同时对异常的
insert into table dm_as.ts_supply_city_od_error_flow_tm partition (inc_month = '$[month(yyyyMM,-1)]')
select product_code,product_name,src_code,src_city,dest_code,dest_city,error_type
  from 
       (select product_code,product_name,src_code,src_city,dest_code,dest_city,add_mark
              ,case when od_tm_mid/avg_near_od_tm_mid-1>0.5 and add_mark=1 then '时效超过周边流向' 
                    when od_tm_mid/avg_near_od_tm_mid-1<-0.5 and src_city not in ('北京市','常州市','成都市','大连市','东莞市','佛山市','福州市','广州市','贵阳市','哈尔滨市','海口市','杭州市','合肥市','呼和浩特市','惠州市','济南市','嘉兴市','金华市','昆明市','拉萨市','兰州市','南昌市','南京市','南宁市','南通市','宁波市','青岛市','泉州市','厦门市','上海市','绍兴市','深圳市','沈阳市','石家庄市','苏州市','台州市','太原市','天津市','温州市','乌鲁木齐市','无锡市','武汉市','西安市','西宁市','徐州市','烟台市','扬州市','银川市','长春市','长沙市','郑州市','中山市','重庆市','珠海市','崇左市','眉山市','资阳市','抚顺市','铁岭市','咸阳市') 
                         then '时效低于周边流向' --一二线城市和省会城市的运输本身就比较快，可以不用考虑时效低的情况
                    else '' end error_type
          from 
               (select product_code,product_name,src_code,src_city,dest_code,dest_city,add_mark,od_tm_mid
                      ,avg(case when rank_same_province<=3 then near_od_tm_mid else null end) as avg_near_od_tm_mid
                  from 
                       (select a.product_code,a.product_name,a.src_code,a.src_city,a.dest_code,a.dest_city,a.od_tm_mid,a.add_mark,b.near_od_tm_mid
                              ,row_number() over(PARTITION BY a.product_code,a.src_city,a.dest_city ORDER BY rank_same_province) as rank_same_province --重新排序，确保取到的周边城市的rank是连续的
                          from 
                               (select a.product_code,a.product_name,a.src_code,a.src_city,a.dest_code,a.dest_city,a.od_tm_mid,a.add_mark
                                      ,b.near_src_city,b.rank_same_province
                                  from 
                                       (select product_code,product_name,src_code,src_city,dest_code,dest_city,od_tm_mid,add_mark
                                          from dm_as.ts_supply_org_city_name_od_tm a  --合并时效和价格并拆分城市后的结果表
                                         where inc_month = '$[month(yyyyMM,-1)]'
                                           and od_tm_mid>0.1
                                           and distance_section not like '%同城%'
                                         group by product_code,product_name,src_code,src_city,dest_code,dest_city,od_tm_mid,add_mark
                                       ) a
                                       
                                       join 

                                       (select sf_city
                                              ,near_sf_city as near_src_city
                                              ,rank_same_province
                                          from dm_as.ts_supply_sfcity_nearby_rank  --顺丰城市及其周边城市
                                       ) b on a.src_city=b.sf_city
                               ) a
                               
                               join 

                               (select product_code,product_name,src_code,src_city,dest_code,dest_city
                                       ,od_tm_mid as near_od_tm_mid
                                  from dm_as.ts_supply_org_city_name_od_tm a
                                  where inc_month = '$[month(yyyyMM,-1)]'
                                    and od_tm_mid>0.1
                                    and distance_section not like '%同城%'
                                  group by product_code,product_name,src_code,src_city,dest_code,dest_city,od_tm_mid
                                ) b on a.product_code=b.product_code and a.near_src_city=b.src_city and a.dest_city=b.dest_city
                        ) t
                  group by product_code,product_name,src_code,src_city,dest_code,dest_city,add_mark,od_tm_mid
               ) t
       ) a
 where not error_type='';
      
--异常流向替代时效表
--异常流向，找到同省最近的，没有异常的流向，关联其流向失效，作为替代数据
alter table dm_as.ts_supply_city_od_price_error_flow_replace_tm DROP IF EXISTS PARTITION (inc_month='$[month(yyyyMM,-1)]');
insert overwrite table dm_as.ts_supply_city_od_price_error_flow_replace_tm partition (inc_month = '$[month(yyyyMM,-1)]')
select a.product_code,a.product_name,a.src_code,a.src_city,a.dest_code,a.dest_city,a.error_type,a.new_src_city
       ,b.avg_od_tm,b.od_tm_05,b.od_tm_mid,b.od_tm_95,b.od_tm_mode,b.od_tm_25,b.od_tm_75,b.intm_24_rt,b.intm_48_rt,b.intm_72_rt --用新的数据替换
       ,od_tm_40,od_tm_45,od_tm_55,od_tm_60,od_tm_65,od_tm_70,od_tm_80,od_tm_85,od_tm_90,intm_36_rt,intm_42_rt,intm_54_rt,intm_60_rt,intm_66_rt,intm_78_rt
       ,intm_84_rt,intm_90_rt,intm_96_rt,intm_2D12_rt,intm_2D18_rt,intm_3D12_rt,intm_3D18_rt,intm_4D12_rt,intm_4D18_rt,intm_that_rt,intm_next_rt,intm_other_rt,wh_od_tm_85
  from 
       (select product_code,product_name,src_code,src_city,dest_code,dest_city,error_type,new_src_city
          from 
               (select a.product_code,a.product_name,a.src_code,a.src_city,a.dest_code,a.dest_city,a.error_type,a.new_src_city
                      ,row_number() over(PARTITION BY a.product_code,a.src_city,a.dest_city ORDER BY rank_same_province) as rank_same_province 
                  from 
                       (select a.product_code,a.product_name,a.src_code,a.src_city,a.dest_code,a.dest_city,a.error_type  --关联最近的城市
                              ,b.new_src_city,b.rank_same_province
                          from 
                               (select *
                                  from dm_as.ts_supply_city_od_error_flow_tm a
                                  where inc_month = '$[month(yyyyMM,-1)]'
                                ) a
                                
                                join 

                                (select sf_city,near_sf_city as new_src_city,rank_same_province
                                   from dm_as.ts_supply_sfcity_nearby_rank
                                ) b on a.src_city=b.sf_city
                          where not b.new_src_city=a.dest_city      --剔除同城数据用来做替换
                       ) a
                       
                       left outer join 

                       (select *
                          from dm_as.ts_supply_city_od_error_flow_tm a
                         where inc_month = '$[month(yyyyMM,-1)]'
                       ) b on a.product_name=b.product_name and a.new_src_city=b.src_city and a.dest_city=b.dest_city     --匹配要替换的数据
                 where b.src_city is null  --剔除已经异常的流向，不作为替换数据
               ) a
         where rank_same_province=1
       ) a
       
       join 

       (select product_code,product_name,src_city,dest_city
              ,max(avg_od_tm) avg_od_tm
              ,max(od_tm_05) od_tm_05
              ,max(od_tm_mid) od_tm_mid
              ,max(od_tm_95) od_tm_95
              ,max(od_tm_mode) od_tm_mode
              ,max(od_tm_25) od_tm_25
              ,max(od_tm_75) od_tm_75
              ,max(intm_24_rt) intm_24_rt
              ,max(intm_48_rt) intm_48_rt
              ,max(intm_72_rt) intm_72_rt
              ,max(od_tm_40) as od_tm_40
              ,max(od_tm_45) as od_tm_45
              ,max(od_tm_55) as od_tm_55
              ,max(od_tm_60) as od_tm_60
              ,max(od_tm_65) as od_tm_65
              ,max(od_tm_70) as od_tm_70
              ,max(od_tm_80) as od_tm_80
              ,max(od_tm_85) as od_tm_85
              ,max(od_tm_90) as od_tm_90
              ,max(intm_36_rt) as intm_36_rt 
              ,max(intm_42_rt) as intm_42_rt 
              ,max(intm_54_rt) as intm_54_rt 
              ,max(intm_60_rt) as intm_60_rt 
              ,max(intm_66_rt) as intm_66_rt 
              ,max(intm_78_rt) as intm_78_rt 
              ,max(intm_84_rt) as intm_84_rt 
              ,max(intm_90_rt) as intm_90_rt 
              ,max(intm_96_rt) as intm_96_rt 
              ,max(intm_2D12_rt) as intm_2D12_rt 
              ,max(intm_2D18_rt) as intm_2D18_rt 
              ,max(intm_3D12_rt) as intm_3D12_rt 
              ,max(intm_3D18_rt) as intm_3D18_rt 
              ,max(intm_4D12_rt) as intm_4D12_rt 
              ,max(intm_4D18_rt) as intm_4D18_rt 
              ,max(intm_that_rt) as intm_that_rt 
              ,max(intm_next_rt) as intm_next_rt 
              ,max(intm_other_rt) as intm_other_rt
              ,max(wh_od_tm_85) as wh_od_tm_85
          from dm_as.ts_supply_org_city_name_od_tm a
         where inc_month = '$[month(yyyyMM,-1)]'
         group by product_code,product_name,src_city,dest_city
       ) b on a.product_code=b.product_code and a.product_name=b.product_name and a.new_src_city=b.src_city and a.dest_city=b.dest_city;

--清洗后的流向时效表
--将异常的流向用正常的时效替代
alter table dm_as.ts_supply_city_od_price_pure_tm DROP IF EXISTS PARTITION (inc_month='$[month(yyyyMM,-1)]');
insert overwrite table dm_as.ts_supply_city_od_price_pure_tm partition(inc_month='$[month(yyyyMM,-1)]')
select /*+mapjoin(b)*/ a.src_code,a.src_city,a.src_province,a.src_dist,a.src_area,a.dest_code,a.dest_city,a.dest_province,a.dest_dist,a.dest_area,a.distance,a.distance_section,a.product_code,a.product_name
       ,case when b.src_city is null then a.add_mark else 2 end add_mark
       ,case when b.src_city is null then a.avg_od_tm else b.avg_od_tm end avg_od_tm
       ,case when b.src_city is null then a.od_tm_05 else b.od_tm_05 end od_tm_05
       ,case when b.src_city is null then a.od_tm_mid else b.od_tm_mid end od_tm_mid
       ,case when b.src_city is null then a.od_tm_95 else b.od_tm_95 end od_tm_95
       ,case when b.src_city is null then a.od_tm_mode else b.od_tm_mode end od_tm_mode
       ,a.pay_country,a.price_code,a.base_weight_qty,a.base_price,a.weight_price_qty,a.max_weight_qty,a.light_factor,a.od_normal
       ,case when b.src_city is null then a.od_tm_25 else b.od_tm_25 end od_tm_25
       ,case when b.src_city is null then a.od_tm_75 else b.od_tm_75 end od_tm_75
       ,case when b.src_city is null then a.intm_24_rt else b.intm_24_rt end intm_24_rt
       ,case when b.src_city is null then a.intm_48_rt else b.intm_48_rt end intm_48_rt
       ,case when b.src_city is null then a.intm_72_rt else b.intm_72_rt end intm_72_rt
       ,a.wh_avg_od_tm,a.wh_od_tm_05,a.wh_od_tm_mid,a.wh_od_tm_95,a.wh_od_tm_mode,a.wh_od_tm_25,a.wh_od_tm_75,a.wh_intm_24_rt,a.wh_intm_48_rt,a.wh_intm_72_rt --顺丰仓的数据不进行替换
       ,case when b.src_city is null then a.od_tm_40 else b.od_tm_40 end as od_tm_40 
       ,case when b.src_city is null then a.od_tm_45 else b.od_tm_45 end as od_tm_45 
       ,case when b.src_city is null then a.od_tm_55 else b.od_tm_55 end as od_tm_55 
       ,case when b.src_city is null then a.od_tm_60 else b.od_tm_60 end as od_tm_60 
       ,case when b.src_city is null then a.od_tm_65 else b.od_tm_65 end as od_tm_65 
       ,case when b.src_city is null then a.od_tm_70 else b.od_tm_70 end as od_tm_70 
       ,case when b.src_city is null then a.od_tm_80 else b.od_tm_80 end as od_tm_80 
       ,case when b.src_city is null then a.od_tm_85 else b.od_tm_85 end as od_tm_85 
       ,case when b.src_city is null then a.od_tm_90 else b.od_tm_90 end as od_tm_90 
       ,case when b.src_city is null then a.intm_36_rt else b.intm_36_rt end as intm_36_rt  
       ,case when b.src_city is null then a.intm_42_rt else b.intm_42_rt end as intm_42_rt  
       ,case when b.src_city is null then a.intm_54_rt else b.intm_54_rt end as intm_54_rt  
       ,case when b.src_city is null then a.intm_60_rt else b.intm_60_rt end as intm_60_rt  
       ,case when b.src_city is null then a.intm_66_rt else b.intm_66_rt end as intm_66_rt  
       ,case when b.src_city is null then a.intm_78_rt else b.intm_78_rt end as intm_78_rt  
       ,case when b.src_city is null then a.intm_84_rt else b.intm_84_rt end as intm_84_rt  
       ,case when b.src_city is null then a.intm_90_rt else b.intm_90_rt end as intm_90_rt  
       ,case when b.src_city is null then a.intm_96_rt else b.intm_96_rt end as intm_96_rt  
       ,case when b.src_city is null then a.intm_2D12_rt else b.intm_2D12_rt end as intm_2D12_rt  
       ,case when b.src_city is null then a.intm_2D18_rt else b.intm_2D18_rt end as intm_2D18_rt  
       ,case when b.src_city is null then a.intm_3D12_rt else b.intm_3D12_rt end as intm_3D12_rt  
       ,case when b.src_city is null then a.intm_3D18_rt else b.intm_3D18_rt end as intm_3D18_rt  
       ,case when b.src_city is null then a.intm_4D12_rt else b.intm_4D12_rt end as intm_4D12_rt  
       ,case when b.src_city is null then a.intm_4D18_rt else b.intm_4D18_rt end as intm_4D18_rt  
       ,case when b.src_city is null then a.intm_that_rt else b.intm_that_rt end as intm_that_rt  
       ,case when b.src_city is null then a.intm_next_rt else b.intm_next_rt end as intm_next_rt  
       ,case when b.src_city is null then a.intm_other_rt else b.intm_other_rt end as intm_other_rt 
       ,a.wh_od_tm_85
  from 
       (select *
          from dm_as.ts_supply_org_city_name_od_tm a        --原始的时效数据
         where inc_month = '$[month(yyyyMM,-1)]'
       ) a
       
       left outer join 

       (select *
          from dm_as.ts_supply_city_od_price_error_flow_replace_tm a        --异常流向替代时效表
          where inc_month = '$[month(yyyyMM,-1)]'
        ) b on a.product_code=b.product_code and a.src_city=b.src_city and a.dest_city=b.dest_city;
  