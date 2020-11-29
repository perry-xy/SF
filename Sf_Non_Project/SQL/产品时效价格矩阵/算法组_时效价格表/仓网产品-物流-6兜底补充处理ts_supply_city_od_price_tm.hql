--兜底逻辑处理
create Table if not exists dm_as.ts_supply_city_od_price_tm 
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
    ,product_is_valid int comment '物流产品是否有效'
    ,product_user_type int comment '物流产品使用者类型，1基础用户'
)comment '供应链产品价格时效表'
partitioned by (inc_month string);

create Table if not exists dm_as.ts_supply_code_od_price_tm 
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
    ,product_is_valid int comment '物流产品是否有效'
    ,product_user_type int comment '物流产品使用者类型，1基础用户'
)comment '供应链产品价格时效表(城市编码唯一)'
partitioned by (inc_month string);

--供应链产品价格时效表
--没有流向服务价格的数据，用顺丰标快兜底
--医药安心递H1，医药专车SE0076，医药专递H2，重货快运C2，参考标快时效
--汇票专送F1，保单配送F3，寿险专送F2，参考标快时效
--医药零担SE0075参考小票零担SE0101时效
alter table dm_as.ts_supply_city_od_price_tm DROP IF EXISTS PARTITION (inc_month='$[month(yyyyMM,-1)]');
insert overwrite table dm_as.ts_supply_city_od_price_tm partition(inc_month='$[month(yyyyMM,-1)]')
select /*+mapjoin(b)*/ a.src_code,a.src_city,a.src_province,a.src_dist,a.src_area,a.dest_code,a.dest_city,a.dest_province,a.dest_dist,a.dest_area,a.distance,a.distance_section,a.product_code,a.product_name
       ,case when a.product_name in ('医药安心递','医药专车','医药专递','重货快运','医药零担','汇票专送','保单配送','寿险专送') then cast(3 as bigint) else a.add_mark end add_mark     --addmark记录为3
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.avg_od_tm   --医药零担时效用小票零担替代，下同
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.avg_od_tm   --其他几个产品，用标快时效替代
             when a.od_normal=1 then a.avg_od_tm
             else b.avg_od_tm end avg_od_tm     --如果od_normal
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_05   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_05   
             when a.od_normal=1 then a.od_tm_05
             else b.od_tm_05 end od_tm_05
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_mid   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_mid   
             when a.od_normal=1 then a.od_tm_mid
             else b.od_tm_mid end od_tm_mid
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_95   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_95   
             when a.od_normal=1 then a.od_tm_95
             else b.od_tm_95 end od_tm_95
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_mode   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_mode   
             when a.od_normal=1 then a.od_tm_mode
             else b.od_tm_mode end od_tm_mode
       ,case when a.od_normal=1 then a.pay_country else b.pay_country end pay_country
       ,case when a.od_normal=1 then a.price_code else b.price_code end price_code
       ,case when a.od_normal=1 then a.base_weight_qty else b.base_weight_qty end base_weight_qty
       ,case when a.od_normal=1 then a.base_price else b.base_price end base_price
       ,case when a.od_normal=1 then a.weight_price_qty else b.weight_price_qty end weight_price_qty
       ,case when a.od_normal=1 then a.max_weight_qty else b.max_weight_qty end max_weight_qty
       ,case when a.od_normal=1 then a.light_factor else b.light_factor end light_factor
       ,a.od_normal
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_25   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_25   
             when a.od_normal=1 then a.od_tm_25
             else b.od_tm_25 end od_tm_25
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_75   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_75   
             when a.od_normal=1 then a.od_tm_75
             else b.od_tm_75 end od_tm_75
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_24_rt   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_24_rt   
             when a.od_normal=1 then a.intm_24_rt
             else b.intm_24_rt end intm_24_rt
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_48_rt   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_48_rt   
             when a.od_normal=1 then a.intm_48_rt
             else b.intm_48_rt end intm_48_rt
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_72_rt   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_72_rt   
             when a.od_normal=1 then a.intm_72_rt
             else b.intm_72_rt end intm_72_rt
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_avg_od_tm   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_avg_od_tm   
             when a.od_normal=1 then a.wh_avg_od_tm
             else b.wh_avg_od_tm end wh_avg_od_tm
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_od_tm_05   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_od_tm_05   
             when a.od_normal=1 then a.wh_od_tm_05
             else b.wh_od_tm_05 end wh_od_tm_05
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_od_tm_mid   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_od_tm_mid   
             when a.od_normal=1 then a.wh_od_tm_mid
             else b.wh_od_tm_mid end wh_od_tm_mid
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_od_tm_95   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_od_tm_95   
             when a.od_normal=1 then a.wh_od_tm_95
             else b.wh_od_tm_95 end wh_od_tm_95
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_od_tm_mode   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_od_tm_mode   
             when a.od_normal=1 then a.wh_od_tm_mode
             else b.wh_od_tm_mode end wh_od_tm_mode
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_od_tm_25   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_od_tm_25   
             when a.od_normal=1 then a.wh_od_tm_25
             else b.wh_od_tm_25 end wh_od_tm_25
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_od_tm_75   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_od_tm_75   
             when a.od_normal=1 then a.wh_od_tm_75
             else b.wh_od_tm_75 end wh_od_tm_75
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_intm_24_rt   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_intm_24_rt   
             when a.od_normal=1 then a.wh_intm_24_rt
             else b.wh_intm_24_rt end wh_intm_24_rt
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_intm_48_rt   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_intm_48_rt   
             when a.od_normal=1 then a.wh_intm_48_rt
             else b.wh_intm_48_rt end wh_intm_48_rt
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_intm_72_rt   
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_intm_72_rt   
             when a.od_normal=1 then a.wh_intm_72_rt
             else b.wh_intm_72_rt end wh_intm_72_rt            
       ,cast(1 as int) product_is_valid
       ,cast(1 as int) product_user_type
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_40
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_40   
             when a.od_normal=1 then a.od_tm_40
             else b.od_tm_40 end as od_tm_40
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_45
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_45   
             when a.od_normal=1 then a.od_tm_45
             else b.od_tm_45 end as od_tm_45
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_55
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_55   
             when a.od_normal=1 then a.od_tm_55
             else b.od_tm_55 end as od_tm_55
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_60
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_60   
             when a.od_normal=1 then a.od_tm_60
             else b.od_tm_60 end as od_tm_60
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_65
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_65   
             when a.od_normal=1 then a.od_tm_65
             else b.od_tm_65 end as od_tm_65
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_70
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_70   
             when a.od_normal=1 then a.od_tm_70
             else b.od_tm_70 end as od_tm_70
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_80
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_80   
             when a.od_normal=1 then a.od_tm_80
             else b.od_tm_80 end as od_tm_80
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_85
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_85   
             when a.od_normal=1 then a.od_tm_85
             else b.od_tm_85 end as od_tm_85
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.od_tm_90
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.od_tm_90   
             when a.od_normal=1 then a.od_tm_90
             else b.od_tm_90 end as od_tm_90
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_36_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_36_rt   
             when a.od_normal=1 then a.intm_36_rt
             else b.intm_36_rt end as intm_36_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_42_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_42_rt   
             when a.od_normal=1 then a.intm_42_rt
             else b.intm_42_rt end as intm_42_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_54_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_54_rt   
             when a.od_normal=1 then a.intm_54_rt
             else b.intm_54_rt end as intm_54_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_60_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_60_rt   
             when a.od_normal=1 then a.intm_60_rt
             else b.intm_60_rt end as intm_60_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_66_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_66_rt   
             when a.od_normal=1 then a.intm_66_rt
             else b.intm_66_rt end as intm_66_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_78_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_78_rt   
             when a.od_normal=1 then a.intm_78_rt
             else b.intm_78_rt end as intm_78_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_84_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_84_rt   
             when a.od_normal=1 then a.intm_84_rt
             else b.intm_84_rt end as intm_84_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_90_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_90_rt   
             when a.od_normal=1 then a.intm_90_rt
             else b.intm_90_rt end as intm_90_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_96_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_96_rt   
             when a.od_normal=1 then a.intm_96_rt
             else b.intm_96_rt end as intm_96_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_2D12_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_2D12_rt   
             when a.od_normal=1 then a.intm_2D12_rt
             else b.intm_2D12_rt end as intm_2D12_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_2D18_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_2D18_rt   
             when a.od_normal=1 then a.intm_2D18_rt
             else b.intm_2D18_rt end as intm_2D18_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_3D12_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_3D12_rt   
             when a.od_normal=1 then a.intm_3D12_rt
             else b.intm_3D12_rt end as intm_3D12_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_3D18_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_3D18_rt   
             when a.od_normal=1 then a.intm_3D18_rt
             else b.intm_3D18_rt end as intm_3D18_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_4D12_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_4D12_rt   
             when a.od_normal=1 then a.intm_4D12_rt
             else b.intm_4D12_rt end as intm_4D12_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_4D18_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_4D18_rt   
             when a.od_normal=1 then a.intm_4D18_rt
             else b.intm_4D18_rt end as intm_4D18_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_that_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_that_rt   
             when a.od_normal=1 then a.intm_that_rt
             else b.intm_that_rt end as intm_that_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_next_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_next_rt   
             when a.od_normal=1 then a.intm_next_rt
             else b.intm_next_rt end as intm_next_rt 
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.intm_other_rt
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.intm_other_rt   
             when a.od_normal=1 then a.intm_other_rt
             else b.intm_other_rt end as intm_other_rt
       ,case when a.od_normal=1 and a.product_name in ('医药零担') then c.wh_od_tm_85
             when a.od_normal=1 and a.product_name in ('医药安心递','医药专车','医药专递','重货快运','汇票专送','保单配送','寿险专送') then b.wh_od_tm_85   
             when a.od_normal=1 then a.wh_od_tm_85
             else b.wh_od_tm_85 end as wh_od_tm_85 
  from 
       (select *
          from dm_as.ts_supply_city_od_price_pure_tm a
          where inc_month = '$[month(yyyyMM,-1)]'
       ) a
       
       left outer join 

       (select *
          from dm_as.ts_supply_city_od_price_pure_tm a
         where inc_month = '$[month(yyyyMM,-1)]'
           and product_name='顺丰标快'     --医药安心递，医药专车，医药专递，重货快运，汇票专送，保单配送，寿险专送，用清洗处理后的标快数据填补，包括时效和价格，因为标快个流向只有一条数据，则不用去重
       ) b on a.src_city=b.src_city and a.dest_city=b.dest_city
       
       left outer join 

       (select src_city,dest_city
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
              ,max(wh_avg_od_tm) wh_avg_od_tm
              ,max(wh_od_tm_05) wh_od_tm_05
              ,max(wh_od_tm_mid) wh_od_tm_mid
              ,max(wh_od_tm_95) wh_od_tm_95
              ,max(wh_od_tm_mode) wh_od_tm_mode
              ,max(wh_od_tm_25) wh_od_tm_25
              ,max(wh_od_tm_75) wh_od_tm_75
              ,max(wh_intm_24_rt) wh_intm_24_rt
              ,max(wh_intm_48_rt) wh_intm_48_rt
              ,max(wh_intm_72_rt) wh_intm_72_rt
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
          from dm_as.ts_supply_city_od_price_pure_tm a
         where inc_month = '$[month(yyyyMM,-1)]'
           and product_name='小票零担'     --医药零担参考小票零担时效
         group by src_city,dest_city
       ) c on a.src_city=c.src_city and a.dest_city=c.dest_city;
      
--按照城市编码合并，给线上使用
alter table dm_as.ts_supply_code_od_price_tm DROP IF EXISTS PARTITION (inc_month='$[month(yyyyMM,-1)]');
insert overwrite table dm_as.ts_supply_code_od_price_tm partition(inc_month='$[month(yyyyMM,-1)]')
    select src_code
           ,concat_ws("/",sort_array(collect_set(src_city))) src_city
           ,max(src_province) src_province
           ,max(src_dist) src_dist
           ,max(src_area) src_area
           ,max(dest_code) dest_code
           ,concat_ws("/",sort_array(collect_set(dest_city))) dest_city
           ,max(dest_province) dest_province
           ,max(dest_dist) dest_dist
           ,max(dest_area) dest_area
           ,min(distance) distance
           ,case when max(case when distance_section like '%同城%' then 1 else 0 end)=1 then '同城'
                when min(distance)<=100 then '(0-100]'
                when min(distance)>100 and min(distance)<=200 then '(100-200]'
                when min(distance)>200 and min(distance)<=400 then '(200-400]'
                when min(distance)>400 and min(distance)<=600 then '(400-600]'
                when min(distance)>600 and min(distance)<=800 then '(600-800]'
                when min(distance)>800 and min(distance)<=1200 then '(800-1200]'
                when min(distance)>1200 and min(distance)<=1600 then '(1200-1600]'
                when min(distance)>1600 and min(distance)<=2000 then '(1600-2000]'
                when min(distance)>2000 and min(distance)<=2500 then '(2000-2500]'
                when min(distance)>2500 and min(distance)<=3000 then '(2500-3000]'
                when min(distance)>3000 and min(distance)<=3500 then '(3000-3500]'
                when min(distance)>3500 and min(distance)<=4000 then '(3500-4000]'
                when min(distance)>4000 and min(distance)<=5000 then '(4000-5000]'
                when min(distance)>5000 and min(distance)<=6000 then '(5000-6000]'
                when min(distance)>6000 then '>6000km'
            end distance_section
           ,product_code
           ,max(product_name) product_name
           ,max(add_mark) add_mark
           ,min(avg_od_tm) avg_od_tm
           ,min(od_tm_05) od_tm_05
           ,min(od_tm_mid) od_tm_mid
           ,min(od_tm_95) od_tm_95
           ,min(od_tm_mode) od_tm_mode
           ,max(pay_country) pay_country
           ,max(price_code) price_code
           ,max(base_weight_qty) base_weight_qty
           ,max(base_price) base_price
           ,max(weight_price_qty) weight_price_qty
           ,max(max_weight_qty) max_weight_qty
           ,max(light_factor) light_factor
           ,max(od_normal) od_normal
           ,min(od_tm_25) od_tm_25
           ,min(od_tm_75) od_tm_75
           ,max(intm_24_rt) intm_24_rt
           ,max(intm_48_rt) intm_48_rt
           ,max(intm_72_rt) intm_72_rt
           ,min(wh_avg_od_tm) wh_avg_od_tm
           ,min(wh_od_tm_05) wh_od_tm_05
           ,min(wh_od_tm_mid) wh_od_tm_mid
           ,min(wh_od_tm_95) wh_od_tm_95
           ,min(wh_od_tm_mode) wh_od_tm_mode
           ,min(wh_od_tm_25) wh_od_tm_25
           ,min(wh_od_tm_75) wh_od_tm_75
           ,max(wh_intm_24_rt) wh_intm_24_rt
           ,max(wh_intm_48_rt) wh_intm_48_rt
           ,max(wh_intm_72_rt) wh_intm_72_rt
           ,max(product_is_valid) product_is_valid
           ,max(product_user_type) product_user_type
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
      from dm_as.ts_supply_city_od_price_tm a
      where inc_month = '$[month(yyyyMM,-1)]'
      group by src_code,dest_code,product_code,base_weight_qty