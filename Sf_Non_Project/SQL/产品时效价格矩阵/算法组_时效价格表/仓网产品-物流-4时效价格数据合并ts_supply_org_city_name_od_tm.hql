--参数配置
--set hive.execution.engine=tez;

-------------起始点城市代码唯一，未将城市代码区分
---结果分区表  每月结果
create table if not exists dm_as.ts_supply_org_city_code_od_tm
(
    src_code           string   comment '原寄地代码',
    src_city           string   comment '原寄城市',
    src_province       string   comment '原寄省',
    src_dist           string   comment '始发区',
    src_area           string   comment '原寄大区',
    dest_code          string   comment '目的地代码',
    dest_city          string   comment '目的城市',
    dest_province      string   comment '目的省',
    dest_dist          string   comment '目的区',
    dest_area          string   comment '目的大区',
    distance           string   comment '距离km',
    distance_section   string   comment '距离分段',
    product_code       string   comment '产品代码',
    product_name       string   comment '产品名称',
    add_mark           int      comment '是否补充数据，1是，0否',
    avg_od_tm          double   comment '平均时效hour',
    od_tm_05           double   comment '最快时效hour',
    od_tm_mid          double   comment '中位数时效hour',
    od_tm_95           double   comment '最慢时效hour',
    od_tm_mode         double   comment '众数时效hour',
    pay_country        string   comment '付款地国家',
    price_code         string   comment '价格区码',
    base_weight_qty    double   comment '首重',
    base_price         double   comment '首重运费',
    weight_price_qty   double   comment '续重运费',
    max_weight_qty     double   comment '最大重量',
    light_factor       double   comment '轻抛',
    od_normal          int      comment '该流向是否有对应产品的价格信息',  
    od_tm_25           double   comment '25百分位时效',
    od_tm_75           double   comment '75百分位时效',	 
    intm_24_rt         double   comment '24小时达成率',
    intm_48_rt         double   comment '48小时达成率',
    intm_72_rt	       double   comment '72小时达成率',
    wh_avg_od_tm     double comment '顺丰仓发出-平均时效hour',
    wh_od_tm_05      double comment '顺丰仓发出-最快时效hour',
    wh_od_tm_mid     double comment '顺丰仓发出-中位数时效hour',
    wh_od_tm_95      double comment '顺丰仓发出-最慢时效hour',
    wh_od_tm_mode    double comment '顺丰仓发出-众数时效hour',
    wh_od_tm_25      double comment '顺丰仓发出-25百分位时效',
    wh_od_tm_75      double comment '顺丰仓发出-75百分位时效',
    wh_intm_24_rt    double comment '顺丰仓发出-24小时达成率',
    wh_intm_48_rt    double comment '顺丰仓发出-48小时达成率',
    wh_intm_72_rt    double comment '顺丰仓发出-72小时达成率'
)partitioned by (inc_month string) stored as parquet;

alter table dm_as.ts_supply_org_city_code_od_tm drop partition (inc_month = '$[month(yyyyMM,-1)]' );
insert overwrite table dm_as.ts_supply_org_city_code_od_tm  partition (inc_month = '$[month(yyyyMM,-1)]' )
select a.src_code
      ,a.src_city
      ,a.src_province
      ,a.src_dist
      ,a.src_area
      ,a.dest_code
      ,a.dest_city
      ,a.dest_province
      ,a.dest_dist
      ,a.dest_area
      ,a.distance
      ,a.distance_section
      ,a.product_code
      ,a.product_name
      ,a.add_mark
      ,a.avg_od_tm
      ,a.od_tm_05
      ,a.od_tm_mid
      ,a.od_tm_95
      ,a.od_tm_mode
      ,b.pay_country
      ,b.price_code
      ,b.base_weight_qty
      ,b.base_price
      ,b.weight_price_qty
      ,b.max_weight_qty
      ,b.light_factor
      ,b.od_normal
      ,a.od_tm_25
      ,a.od_tm_75
      ,a.intm_24_rt
      ,a.intm_48_rt
      ,a.intm_72_rt	
      ,a.wh_avg_od_tm
      ,a.wh_od_tm_05
      ,a.wh_od_tm_mid
      ,a.wh_od_tm_95
      ,a.wh_od_tm_mode
      ,a.wh_od_tm_25
      ,a.wh_od_tm_75
      ,a.wh_intm_24_rt
      ,a.wh_intm_48_rt
      ,a.wh_intm_72_rt
      ,a.od_tm_40
      ,a.od_tm_45
      ,a.od_tm_55
      ,a.od_tm_60
      ,a.od_tm_65
      ,a.od_tm_70
      ,a.od_tm_80
      ,a.od_tm_85
      ,a.od_tm_90
      ,a.intm_36_rt 
      ,a.intm_42_rt 
      ,a.intm_54_rt 
      ,a.intm_60_rt 
      ,a.intm_66_rt 
      ,a.intm_78_rt 
      ,a.intm_84_rt 
      ,a.intm_90_rt 
      ,a.intm_96_rt 
      ,a.intm_2D12_rt 
      ,a.intm_2D18_rt 
      ,a.intm_3D12_rt 
      ,a.intm_3D18_rt 
      ,a.intm_4D12_rt 
      ,a.intm_4D18_rt 
      ,a.intm_that_rt 
      ,a.intm_next_rt 
      ,a.intm_other_rt
      ,a.wh_od_tm_85
  from 
       (select src_code,src_city,src_province,src_dist,src_area,dest_code,dest_city,dest_province,dest_dist,dest_area,distance,distance_section,product_code,product_name,add_mark,avg_od_tm,od_tm_mode
              ,od_tm_05,od_tm_25,od_tm_40,od_tm_45,od_tm_mid,od_tm_55,od_tm_60,od_tm_65,od_tm_70,od_tm_75,od_tm_80,od_tm_85,od_tm_90,od_tm_95,intm_24_rt,intm_36_rt,intm_42_rt,intm_48_rt,intm_54_rt
              ,intm_60_rt,intm_66_rt,intm_72_rt,intm_78_rt,intm_84_rt,intm_90_rt,intm_96_rt,intm_2D12_rt,intm_2D18_rt,intm_3D12_rt,intm_3D18_rt,intm_4D12_rt,intm_4D18_rt,intm_that_rt,intm_next_rt,intm_other_rt
              ,wh_avg_od_tm,wh_od_tm_mode,wh_od_tm_05,wh_od_tm_25,wh_od_tm_mid,wh_od_tm_75,wh_od_tm_85,wh_od_tm_95,wh_intm_24_rt,wh_intm_48_rt,wh_intm_72_rt              
          from dm_as.ts_supply_way_tm_avg_percentile_m  --合并所有流向和从仓库发货的流向的时效数据
         where inc_month='$[month(yyyyMM,-1)]'
       ) a
       
       left join 

       (select product_code,product_name,src_code,src_name,dest_code,dest_name,pay_country,price_code,base_weight_qty,base_price,weight_price_qty,max_weight_qty,light_factor,od_normal
          from dm_as.ts_supply_product_flow_price_m  --产品流向对应的价格
         where inc_month='$[month(yyyyMM,-1)]'
       ) b on a.product_code=b.product_code and a.src_code=b.src_code and a.dest_code=b.dest_code;

create table if not exists dm_as.ts_supply_org_city_name_od_tm
(
	src_code           string   comment '原寄地代码',
	src_city           string   comment '原寄城市',
	src_province       string   comment '原寄省',
	src_dist           string   comment '始发区',
	src_area           string   comment '原寄大区',
	dest_code          string   comment '目的地代码',
	dest_city          string   comment '目的城市',
	dest_province      string   comment '目的省',
	dest_dist          string   comment '目的区',
	dest_area          string   comment '目的大区',
	distance           string   comment '距离km',
	distance_section   string   comment '距离分段',
	product_code       string   comment '产品代码',
	product_name       string   comment '产品名称',
	add_mark           int      comment '是否补充数据，1是，0否',
	avg_od_tm          double   comment '平均时效hour',
	od_tm_05           double   comment '最快时效hour',
	od_tm_mid          double   comment '中位数时效hour',
	od_tm_95           double   comment '最慢时效hour',
	od_tm_mode         double   comment '众数时效hour',
	pay_country        string   comment '付款地国家',
	price_code         string   comment '价格区码',
	base_weight_qty    double   comment '首重',
	base_price         double   comment '首重运费',
	weight_price_qty   double   comment '续重运费',
	max_weight_qty     double   comment '最大重量',
	light_factor       double   comment '轻抛',
	od_normal          int      comment '该流向是否有对应产品的价格信息',  
	od_tm_25           double   comment '25百分位时效',
	od_tm_75           double   comment '75百分位时效',	 
	intm_24_rt         double   comment '24小时达成率',
	intm_48_rt         double   comment '48小时达成率',
	intm_72_rt	       double   comment '72小时达成率',
	wh_avg_od_tm     double comment '顺丰仓发出-平均时效hour',
	wh_od_tm_05      double comment '顺丰仓发出-最快时效hour',
    wh_od_tm_mid     double comment '顺丰仓发出-中位数时效hour',
	wh_od_tm_95      double comment '顺丰仓发出-最慢时效hour',
	wh_od_tm_mode    double comment '顺丰仓发出-众数时效hour',
	wh_od_tm_25      double comment '顺丰仓发出-25百分位时效',
	wh_od_tm_75      double comment '顺丰仓发出-75百分位时效',
	wh_intm_24_rt    double comment '顺丰仓发出-24小时达成率',
	wh_intm_48_rt    double comment '顺丰仓发出-48小时达成率',
	wh_intm_72_rt    double comment '顺丰仓发出-72小时达成率'
)partitioned by (inc_month string) stored as parquet;

alter table dm_as.ts_supply_org_city_name_od_tm drop partition (inc_month = '$[month(yyyyMM,-1)]' );
insert overwrite table dm_as.ts_supply_org_city_name_od_tm  partition (inc_month = '$[month(yyyyMM,-1)]' )
select src_code
      ,deal_src_city as src_city
      ,src_province
      ,src_dist
      ,src_area
      ,dest_code
      ,deal_dest_city as dest_city
      ,dest_province
      ,dest_dist
      ,dest_area
      ,distance
      ,distance_section
      ,product_code
      ,product_name
      ,add_mark
      ,avg_od_tm
      ,od_tm_05
      ,od_tm_mid
      ,od_tm_95
      ,od_tm_mode
      ,pay_country
      ,price_code
      ,base_weight_qty
      ,base_price
      ,weight_price_qty
      ,max_weight_qty
      ,light_factor
      ,od_normal
      ,od_tm_25
      ,od_tm_75
      ,intm_24_rt
      ,intm_48_rt
      ,intm_72_rt	
      ,wh_avg_od_tm
      ,wh_od_tm_05
      ,wh_od_tm_mid
      ,wh_od_tm_95
      ,wh_od_tm_mode
      ,wh_od_tm_25
      ,wh_od_tm_75
      ,wh_intm_24_rt
      ,wh_intm_48_rt
      ,wh_intm_72_rt
      ,od_tm_40
      ,od_tm_45
      ,od_tm_55
      ,od_tm_60
      ,od_tm_65
      ,od_tm_70
      ,od_tm_80
      ,od_tm_85
      ,od_tm_90
      ,intm_36_rt 
      ,intm_42_rt 
      ,intm_54_rt 
      ,intm_60_rt 
      ,intm_66_rt 
      ,intm_78_rt 
      ,intm_84_rt 
      ,intm_90_rt 
      ,intm_96_rt 
      ,intm_2D12_rt 
      ,intm_2D18_rt 
      ,intm_3D12_rt 
      ,intm_3D18_rt 
      ,intm_4D12_rt 
      ,intm_4D18_rt 
      ,intm_that_rt 
      ,intm_next_rt 
      ,intm_other_rt
      ,wh_od_tm_85
  from 
       (select b.*,deal_src_city
          from 
               (select a.*,deal_dest_city
                  from 
                       (select *
                          from dm_as.ts_supply_org_city_code_od_tm a 
                         where inc_month='$[month(yyyyMM,-1)]'
                       ) a
                      LATERAL VIEW explode(split(dest_city,'\\/')) cc AS deal_dest_city 
               ) b 
               LATERAL VIEW explode(split(src_city,'\\/')) cc AS deal_src_city 
       ) t;