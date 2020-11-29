--导入未兜底前的，2020年8月的，统计了六个月的，城市名唯一的特惠C时效价格表
create Table if not exists thui_C
(
 src_code text comment '原寄地代码'
    ,src_city text comment '原寄地城市'
    ,src_province text comment '原寄省'
    ,src_dist text comment '原寄区'
    ,src_area text comment '原寄大区'
    ,dest_code text comment '目的城市编码'
    ,dest_city text comment '目的城市'
    ,dest_province text comment '目的省'
    ,dest_dist text comment '目的区'
    ,dest_area text comment '目的大区'
    ,distance text comment '距离km'
    ,distance_section text comment '距离分段'
    ,product_code text comment '产品代码'
    ,product_name text comment '产品名称'
    ,add_mark int comment '是否补充数据，1是，0否，2经过异常处理'
    ,avg_od_tm double comment '平均时效hour'
    ,od_tm_05 double comment '最快时效hour'
    ,od_tm_mid double comment '中位数时效hour'
    ,od_tm_95 double comment '最慢时效hour'
    ,od_tm_mode double comment '众数时效hour'
    ,pay_country text comment '付款地国家'
    ,price_code text comment '价格区码'
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
    ,product_is_valid bigint comment '物流产品是否有效'
    ,product_user_type bigint comment '物流产品使用者类型，1基础用户',
    od_tm_40  double comment '40百分位时效',
    od_tm_45 double comment '45百分位时效',
    od_tm_55 double comment '55百分位时效',
    od_tm_60 double comment '60百分位时效',
    od_tm_65 double comment '65百分位时效',
    od_tm_70 double comment '70百分位时效',
    od_tm_80 double comment '80百分位时效',
    od_tm_85 double comment '85百分位时效',
    od_tm_90 double comment '90百分位时效',
    intm_36_rt double comment '36小时达成率',
    intm_42_rt double comment '42小时达成率',
    intm_54_rt double comment '54小时达成率',
    intm_60_rt double comment '60小时达成率',
    intm_66_rt double comment '66小时达成率',
    intm_78_rt double comment '78小时达成率',
    intm_84_rt double comment '84小时达成率',
    intm_90_rt double comment '90小时达成率',
    intm_96_rt double comment '96小时达成率',
    intm_2D12_rt double comment '2D12达成率',
    intm_2D18_rt double comment '2D18达成率',
    intm_3D12_rt double comment '3D12达成率', 
    intm_3D18_rt double comment '3D18达成率',
    intm_4D12_rt double comment '4D12达成率',
    intm_4D18_rt double comment '4D18达成率',
    intm_that_rt double comment '当日达成率',
    intm_next_rt double comment '次日达成率',
    intm_other_rt double comment '隔日达成率',
    wh_od_tm_85  double comment '顺丰仓发出-85百分位时效'
)comment '顺丰特惠C未兜底前数据-城市名唯一-202008';

drop table province_province_section;
--建省份-省份-距离的均值表
CREATE TABLE province_province_section(
    src_province text,
	dest_province text,
	distance_section text,
    pay_country text,
    price_code text,
    base_weight_qty double comment '首重',
    base_price double comment '首重运费',
    weight_price_qty double comment '续重运费',
    max_weight_qty double comment '最大重量',
    light_factor double comment '轻抛'
) COMMENT='省-省-距离区间-平均';

truncate table province_province_section;
insert into province_province_section
select src_province,
	dest_province,
	distance_section,
    max(pay_country) pay_country,
    max(price_code) price_code,
    avg(base_weight_qty) base_weight_qty,
    avg(base_price) base_price,
    avg(weight_price_qty) weight_price_qty,
    avg(max_weight_qty) max_weight_qty,
    avg(light_factor) light_factor
from thui_c
where base_weight_qty=1 and od_normal=1
group by src_province,
dest_province,
distance_section;

--建区域间的表
CREATE TABLE area_area_section(
  src_area text,
	dest_area text,
	distance_section text,
    pay_country text,
    price_code text,
    base_weight_qty double comment '首重',
    base_price double comment '首重运费',
    weight_price_qty double comment '续重运费',
    max_weight_qty double comment '最大重量',
    light_factor double comment '轻抛'
) COMMENT='区域-区域-距离区间-平均';

truncate table area_area_section;
insert into area_area_section
select src_area,
	dest_area,
	distance_section,
    max(pay_country) pay_country,
    max(price_code) price_code,
    avg(base_weight_qty) base_weight_qty,
    avg(base_price) base_price,
    avg(weight_price_qty) weight_price_qty,
    avg(max_weight_qty) max_weight_qty,
    avg(light_factor) light_factor
from thui_c
where base_weight_qty=1 and od_normal=1
group by src_area,
	dest_area,
	distance_section;

--建距离区间的表
CREATE TABLE section(
	distance_section text,
    pay_country text,
    price_code text,
    base_weight_qty double comment '首重',
    base_price double comment '首重运费',
    weight_price_qty double comment '续重运费',
    max_weight_qty double comment '最大重量',
    light_factor double comment '轻抛'
) COMMENT='距离区间-平均';

truncate table section;
insert into section
select 
	distance_section,
    max(pay_country) pay_country,
    max(price_code) price_code,
    avg(base_weight_qty) base_weight_qty,
    avg(base_price) base_price,
    avg(weight_price_qty) weight_price_qty,
    avg(max_weight_qty) max_weight_qty,
    avg(light_factor) light_factor
from thui_c
where base_weight_qty=1  and od_normal=1
group by distance_section;

--填充没有流向服务的流向的价格
select 
     a.src_code,a.src_city ,a.src_province ,a.src_dist ,a.src_area,a.dest_code ,a.dest_city ,a.dest_province,a.dest_dist ,a.dest_area ,a.distance
    ,a.distance_section,a.product_code,a.product_name,a.add_mark
    ,avg_od_tm,od_tm_05,od_tm_mid,od_tm_95,od_tm_mode
    ,ifnull(ifnull(b.pay_country,c.pay_country),d.pay_country) pay_country
    ,ifnull(ifnull(b.price_code,c.price_code),d.price_code) price_code
    ,ifnull(ifnull(ifnull(a.base_weight_qty,b.base_weight_qty),c.base_weight_qty),d.base_weight_qty) base_weight_qty
    ,ifnull(ifnull(ifnull(a.base_price,b.base_price),c.base_price),d.base_price) base_price
    ,ifnull(ifnull(ifnull(a.weight_price_qty,b.weight_price_qty),c.weight_price_qty),d.weight_price_qty) weight_price_qty
    ,ifnull(ifnull(ifnull(a.max_weight_qty,b.max_weight_qty),c.max_weight_qty),d.max_weight_qty) max_weight_qty
    ,ifnull(ifnull(ifnull(a.light_factor,b.light_factor),c.light_factor),d.light_factor) light_factor
    ,od_normal,od_tm_25,od_tm_75,intm_24_rt,intm_48_rt,intm_72_rt,wh_avg_od_tm,wh_od_tm_05,wh_od_tm_mid,wh_od_tm_95,wh_od_tm_mode
    ,wh_od_tm_25,wh_od_tm_75,wh_intm_24_rt,wh_intm_48_rt,wh_intm_72_rt
    ,product_is_valid
    ,product_user_type,
    od_tm_40,od_tm_45,od_tm_55,od_tm_60,od_tm_65,od_tm_70,od_tm_80,od_tm_85,od_tm_90,
    intm_36_rt,intm_42_rt,intm_54_rt,intm_60_rt,intm_66_rt,intm_78_rt,intm_84_rt,intm_90_rt,intm_96_rt,intm_2D12_rt,intm_2D18_rt,
    intm_3D12_rt, intm_3D18_rt,intm_4D12_rt,intm_4D18_rt,intm_that_rt,intm_next_rt,intm_other_rt,wh_od_tm_85
from 
(select  * from thui_c
where od_normal is null) a
left join 
province_province_section b 
on a.src_province=b.src_province and 
a.dest_province=b.dest_province and 
a.distance_section=b.distance_section
left join 
area_area_section c 
on a.src_area=c.src_area and a.dest_area=c.dest_area and a.distance_section=c.distance_section
left join 
section d 
on a.distance_section=d.distance_section

--城市名合成城市代码唯一
select src_code
           ,src_city
           ,src_province
           ,src_dist
           ,src_area
           ,dest_code
           ,GROUP_CONCAT(dest_city order by dest_city SEPARATOR '/') dest_city
           ,dest_province
           ,dest_dist
           ,dest_area
           ,min(distance) distance
           ,distance_section
           ,product_code
           ,product_name
           ,max(add_mark) add_mark
           ,min(avg_od_tm) avg_od_tm
           ,min(od_tm_05) od_tm_05
           ,min(od_tm_mid) od_tm_mid
           ,min(od_tm_95) od_tm_95
           ,min(od_tm_mode) od_tm_mode
           ,pay_country
           ,price_code
           ,base_weight_qty
           ,base_price
           ,weight_price_qty
           ,max_weight_qty
           ,light_factor
           ,od_normal
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
           ,max(intm_that_rt) as intm_that_rt 
           ,max(intm_next_rt) as intm_next_rt 
           ,max(intm_other_rt) as intm_other_rt
from
(select src_code
           ,GROUP_CONCAT(src_city order by src_city SEPARATOR '/') src_city
           ,src_province
           ,src_dist
           ,src_area
           ,dest_code
		    ,dest_city
           ,dest_province
           ,dest_dist
           ,dest_area
           ,min(distance) distance
           ,distance_section
           ,product_code
           ,product_name
           ,max(add_mark) add_mark
           ,min(avg_od_tm) avg_od_tm
           ,min(od_tm_05) od_tm_05
           ,min(od_tm_mid) od_tm_mid
           ,min(od_tm_95) od_tm_95
           ,min(od_tm_mode) od_tm_mode
           ,pay_country
           ,price_code
           ,base_weight_qty
           ,base_price
           ,weight_price_qty
           ,max_weight_qty
           ,light_factor
           ,od_normal
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
           ,max(intm_that_rt) as intm_that_rt 
           ,max(intm_next_rt) as intm_next_rt 
           ,max(intm_other_rt) as intm_other_rt
      from city_name a
      group by src_code
           ,src_province
           ,src_dist
           ,src_area
           ,dest_code
			,dest_city
           ,dest_province
           ,dest_dist
           ,dest_area
           ##,distance
           ,distance_section
           ,product_code
           ,product_name
           ,pay_country
           ,price_code
           ,base_weight_qty
           ,base_price
           ,weight_price_qty
           ,max_weight_qty
           ,light_factor
           ,od_normal
           ) a
group by src_code
						,src_city
           ,src_province
           ,src_dist
           ,src_area
           ,dest_code
           ,dest_province
           ,dest_dist
           ,dest_area
           ,distance_section
           ,product_code
           ,product_name
           ,pay_country
           ,price_code
           ,base_weight_qty
           ,base_price
           ,weight_price_qty
           ,max_weight_qty
           ,light_factor
           ,od_normal








