set mapreduce.job.queuename=root.ISD;
---------时效调度-----------------------------------------------------
--------------------------------------------------------------------------------------------------------------
/*
流向距离基表，线下数据  --372,笛卡尔积106926
drop table tmp_dm_as.smc_sf_se_price_deal_use_tmp;
create table if not exists tmp_dm_as.smc_sf_se_price_deal_use_tmp(
src_code          string comment '',
src_city          string comment '',
src_province      string comment '',
src_dist          string comment '',
src_area          string comment '',
dest_code         string comment '',
dest_city         string comment '',
dest_province     string comment '',
dest_dist         string comment '',
dest_area         string comment '',
area_type         string comment '',
distance          string comment '',
distance_section  string comment ''
) 
row format delimited fields terminated by ',';
load data inpath '/user/01374548/upload/od_base_data.csv' into table tmp_dm_as.smc_sf_se_price_deal_use_tmp;
	
*/
/*
数据备份20191209
drop table tmp_dm_as.ts_prod_city_name_od_price_tm_bk_201911;  
create table tmp_dm_as.ts_prod_city_name_od_price_tm_bk_201911 stored as parquet as	
select *
from dm_as.ts_prod_city_name_od_price_tm;

drop table tmp_dm_as.ts_prod_city_code_od_price_tm_bk_201911;  
create table tmp_dm_as.ts_prod_city_code_od_price_tm_bk_201911 stored as parquet as	
select *
from dm_as.ts_prod_city_code_od_price_tm;
*/


--*******************************************************--
-------------------------产品时效信息--------------------------
--*******************************************************--
--各流向均值，中位数，百分位数
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp2_1;     
create table tmp_dm_as.ts_prod_od_tm_0701_tmp2_1 stored as parquet as
select 
	product_code,	
	src_dist_code,
	src_city_code,
	dest_dist_code,
	dest_city_code,
	avg(od_tm) as avg_od_tm,
	count(*) as waybill_num      --20191125新增流向数据量限制
from tmp_dm_as.ts_prod_od_tm_0701_tmp1
group by 
	product_code,	
	src_dist_code,
	src_city_code,
	dest_dist_code,
	dest_city_code  ;

--分位数
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp2_2;  
create table tmp_dm_as.ts_prod_od_tm_0701_tmp2_2 stored as parquet as
select 
	product_code,	
	src_dist_code,
	src_city_code,
	dest_dist_code,
	dest_city_code,
	percentile_approx(od_tm,0.05,3000) as od_tm_05,	
	percentile_approx(od_tm,0.5,3000) as od_tm_mid,
	percentile_approx(od_tm,0.95,3000) as od_tm_95,
	percentile_approx(od_tm,0.25,3000) as od_tm_25,
	percentile_approx(od_tm,0.75,3000) as od_tm_75
from tmp_dm_as.ts_prod_od_tm_0701_tmp1
group by 
	product_code,	
	src_dist_code,
	src_city_code,
	dest_dist_code,
	dest_city_code;
 
--各流向众数
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp2_3;    --820429
create table tmp_dm_as.ts_prod_od_tm_0701_tmp2_3 stored as parquet as
select 
	product_code,	
	src_dist_code,
	src_city_code,
	dest_dist_code,
	dest_city_code,
	od_tm_hour as od_tm_mode
from 
	(select *,row_number() over(partition by product_code,src_dist_code,dest_dist_code order by od_tm_num desc) rn 
	from 
		(select
			product_code,	
			src_dist_code,
			src_city_code,
			dest_dist_code,
			dest_city_code,
			round(od_tm,0) as od_tm_hour,
			count(*) as od_tm_num
		from tmp_dm_as.ts_prod_od_tm_0701_tmp1
		group by 
			product_code,	
			src_dist_code,
			src_city_code,	
			dest_dist_code,
			dest_city_code,
			round(od_tm,0)  
	    ) a 
	) b 
where b.rn = 1 ;

--组合  流向时效基础表
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp2;     --820429
create table tmp_dm_as.ts_prod_od_tm_0701_tmp2 stored as parquet as
select 
	a.*,
	b.od_tm_05,	
	b.od_tm_mid,
	b.od_tm_95,
	c.od_tm_mode,
	b.od_tm_25,
	b.od_tm_75
from (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2_1 where waybill_num>=10)  a 
left join tmp_dm_as.ts_prod_od_tm_0701_tmp2_2 b 
	on a.product_code = b.product_code
	and a.src_dist_code = b.src_dist_code
	and a.dest_dist_code = b.dest_dist_code
left join tmp_dm_as.ts_prod_od_tm_0701_tmp2_3 c 
	on a.product_code = c.product_code
	and a.src_dist_code = c.src_dist_code
	and a.dest_dist_code = c.dest_dist_code;



---------------------------------------------------------------------------
---补充数据准备
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp7_1;  
create table tmp_dm_as.ts_prod_od_tm_0701_tmp7_1 stored as parquet as
select 
	a.*,
	b.product_code,
	b.avg_od_tm,
	b.od_tm_05,	
	b.od_tm_mid,
	b.od_tm_95,
	b.od_tm_mode,
	b.od_tm_25,	
	b.od_tm_75
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a       ---基础流向表
left join tmp_dm_as.ts_prod_od_tm_0701_tmp2 b 
	on a.src_code = b.src_dist_code
	and a.dest_code = b.dest_dist_code
where b.product_code is not null;

--各产品  省，距离均值
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp7_2;  
create table tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 stored as parquet as	
select 
	product_code,
	src_province,
	dest_province,
	distance_section,
	avg(avg_od_tm)     as avg_od_tm,
	avg(od_tm_05)	   as od_tm_05,
	avg(od_tm_mid)     as od_tm_mid,
	avg(od_tm_95)      as od_tm_95,
	avg(od_tm_mode)	   as od_tm_mode,	
	avg(od_tm_25)      as od_tm_25,	
	avg(od_tm_75)      as od_tm_75
from tmp_dm_as.ts_prod_od_tm_0701_tmp7_1
group by 
	product_code,
	src_province,
	dest_province,
	distance_section;
	
--各产品   距离均值	
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp7_3;  
create table tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 stored as parquet as	
select 
	product_code,
	distance_section,
	avg(avg_od_tm)     as avg_od_tm,
	avg(od_tm_05)	   as od_tm_05,
	avg(od_tm_mid)     as od_tm_mid,
	avg(od_tm_95)      as od_tm_95,
	avg(od_tm_mode)	   as od_tm_mode,
	avg(od_tm_25)      as od_tm_25,	
	avg(od_tm_75)      as od_tm_75
from tmp_dm_as.ts_prod_od_tm_0701_tmp7_1
group by 
	product_code,
	distance_section;		
	

------------------------------------------------------------------------	
---------------------------------------------------------------------------
--各产品匹配流向
--SE0001	顺丰标快
--SE0003	顺丰次晨
--SE0004	顺丰特惠
--SE0006	物流普运
--SE0020	重货专运
--SE0021	重货快运
--SE0100	重货包裹
--SE0101	小票零担

drop table tmp_dm_as.whl_ts_prod_od_tm_detail;  
create table tmp_dm_as.whl_ts_prod_od_tm_detail stored as parquet as	
select a.*,
	'SE0001' as product_code,
	'顺丰标快' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0001') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0001') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0001') d 
	on a.distance_section = d.distance_section

union all
select a.*,
	'SE0003' as product_code,
	'顺丰次晨' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0003') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0003') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0003') d 
	on a.distance_section = d.distance_section		

union all
select a.*,
	'SE0004' as product_code,
	'顺丰特惠' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from (select * from tmp_dm_as.smc_sf_se_price_deal_use_tmp where src_code <> dest_code) a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0004') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0004') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0004') d 
	on a.distance_section = d.distance_section		

union all
select a.*,
	'SE0004' as product_code,
	'顺丰特惠' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from (select * from tmp_dm_as.smc_sf_se_price_deal_use_tmp where src_code = dest_code ) a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0001' ) b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0001') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0001') d 
	on a.distance_section = d.distance_section	
	
union all
select a.*,
	'SE0006' as product_code,
	'物流普运' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0006') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0006') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0006') d 
	on a.distance_section = d.distance_section	

union all
select a.*,
	'SE0020' as product_code,
	'重货专运' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0020') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0020') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0020') d 
	on a.distance_section = d.distance_section	
	
union all
select a.*,
	'SE0021' as product_code,
	'重货快运' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0021') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0021') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0021') d 
	on a.distance_section = d.distance_section	
	
union all
select a.*,
	'SE0101' as product_code,
	'小票零担' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0101') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0101') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0101') d 
	on a.distance_section = d.distance_section		

union all
select a.*,
	'SE0100' as product_code,
	'重货包裹' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0100') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0100') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0100') d 
	on a.distance_section = d.distance_section	
	
union all
select a.*,
	'SE0117' as product_code,
	'顺丰特惠C' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0117') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0117') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0117') d 
	on a.distance_section = d.distance_section

	union all
select a.*,
	'SE0118' as product_code,
	'顺丰特惠B' as product_name,
	case when b.avg_od_tm is null then 1 else 0 end as add_mark,
	nvl(nvl(b.avg_od_tm  ,c.avg_od_tm  ), d.avg_od_tm  )  as avg_od_tm  ,
	nvl(nvl(b.od_tm_05   ,c.od_tm_05   ), d.od_tm_05   )  as od_tm_05   ,
	nvl(nvl(b.od_tm_mid  ,c.od_tm_mid  ), d.od_tm_mid  )  as od_tm_mid  ,
	nvl(nvl(b.od_tm_95   ,c.od_tm_95   ), d.od_tm_95   )  as od_tm_95   ,
	nvl(nvl(b.od_tm_mode ,c.od_tm_mode ), d.od_tm_mode )  as od_tm_mode,
	nvl(nvl(b.od_tm_25   ,c.od_tm_25   ), d.od_tm_25   )  as od_tm_25   ,
	nvl(nvl(b.od_tm_75   ,c.od_tm_75   ), d.od_tm_75   )  as od_tm_75 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp2 where product_code = 'SE0118') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_2 where product_code = 'SE0118') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp7_3 where product_code = 'SE0118') d 
	on a.distance_section = d.distance_section
;		
	
----------------*****************************************------------------------
----各流向达成率-24、48、72---------------------
--各流向时效达成
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp3_2;     
create table tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 stored as parquet as
select 
	product_code,
	src_dist_code,
	dest_dist_code,
	intm_24_num,
	intm_48_num,
	intm_72_num,
	tot_num,
	intm_24_num / tot_num as intm_24_rt,
	intm_48_num / tot_num as intm_48_rt,
	intm_72_num / tot_num as intm_72_rt
from 
	(select 
		product_code,	
		src_dist_code,
		dest_dist_code,
		sum(intm_24) as intm_24_num,
		sum(intm_48) as intm_48_num,	
		sum(intm_72) as intm_72_num,	
		count(*) as tot_num
	from tmp_dm_as.ts_prod_od_tm_0701_tmp1
	group by 
		product_code,	
		src_dist_code,
		dest_dist_code )t
where t.tot_num >= 10;

----各产品  省时效达成数据准备
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp3_3;     
create table tmp_dm_as.ts_prod_od_tm_0701_tmp3_3 stored as parquet as	
select 
	a.*,
	b.product_code,
	b.src_dist_code,
	b.dest_dist_code,
	b.intm_24_num,
	b.intm_48_num,
	b.intm_72_num,
	b.tot_num,
	b.intm_24_rt,
	b.intm_48_rt,
	b.intm_72_rt
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a       ---基础流向表
left join tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 b 
	on a.src_code = b.src_dist_code
	and a.dest_code = b.dest_dist_code
where b.product_code is not null;

--各产品  省时效达成
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp3_4;     
create table tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 stored as parquet as
select 
	t.*,
	intm_24_num / tot_num as intm_24_rt,
	intm_48_num / tot_num as intm_48_rt,
	intm_72_num / tot_num as intm_72_rt
from 
	(select
		product_code,
		src_province,
		dest_province,
		distance_section,
		sum(intm_24_num) as intm_24_num,
		sum(intm_48_num) as intm_48_num,
		sum(intm_72_num) as intm_72_num,
		sum(tot_num) as tot_num
	from tmp_dm_as.ts_prod_od_tm_0701_tmp3_3
	group by 
		product_code,
		src_province,
		dest_province,
		distance_section )t ;

--各产品时效达成
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp3_5;     
create table tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 stored as parquet as
select 
	t.*,
	intm_24_num / tot_num as intm_24_rt,
	intm_48_num / tot_num as intm_48_rt,
	intm_72_num / tot_num as intm_72_rt
from 
	(select
		product_code,
		distance_section,
		sum(intm_24_num) as intm_24_num,
		sum(intm_48_num) as intm_48_num,
		sum(intm_72_num) as intm_72_num,
		sum(tot_num) as tot_num
	from tmp_dm_as.ts_prod_od_tm_0701_tmp3_3
	group by 
		product_code,
		distance_section )t ;
		
--组合	
drop table tmp_dm_as.whl_ts_prod_od_rt_detail2;     
create table tmp_dm_as.whl_ts_prod_od_rt_detail2 stored as parquet as	
select a.*,
	'SE0001' as product_code,
	'顺丰标快' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0001') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0001') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0001') d 
	on a.distance_section = d.distance_section

union all
select a.*,
	'SE0003' as product_code,
	'顺丰次晨' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0003') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0003') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0003') d 
	on a.distance_section = d.distance_section		

union all
select a.*,
	'SE0004' as product_code,
	'顺丰特惠' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from (select * from tmp_dm_as.smc_sf_se_price_deal_use_tmp where src_code <> dest_code ) a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0004' ) b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0004') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0004') d 
	on a.distance_section = d.distance_section		

union all
select a.*,
	'SE0004' as product_code,
	'顺丰特惠' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from (select * from tmp_dm_as.smc_sf_se_price_deal_use_tmp where src_code = dest_code) a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0001') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0001') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0001') d 
	on a.distance_section = d.distance_section		
	
	
union all
select a.*,
	'SE0006' as product_code,
	'物流普运' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0006') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0006') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0006') d 
	on a.distance_section = d.distance_section	

union all
select a.*,
	'SE0020' as product_code,
	'重货专运' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0020') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0020') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0020') d 
	on a.distance_section = d.distance_section	
	
union all
select a.*,
	'SE0021' as product_code,
	'重货快运' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0021') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0021') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0021') d 
	on a.distance_section = d.distance_section	
	
union all
select a.*,
	'SE0101' as product_code,
	'小票零担' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0101') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0101') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0101') d 
	on a.distance_section = d.distance_section		

union all
select a.*,
	'SE0100' as product_code,
	'重货包裹' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0100') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0100') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0100') d 
	on a.distance_section = d.distance_section	
	
union all
select a.*,
	'SE0117' as product_code,
	'顺丰特惠C' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0117') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0117') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0117') d 
	on a.distance_section = d.distance_section

	union all
select a.*,
	'SE0118' as product_code,
	'顺丰特惠B' as product_name,
	case when b.intm_24_rt is null then 1 else 0 end as add_mark,
	nvl(nvl(b.intm_24_rt, c.intm_24_rt  ), d.intm_24_rt  )  as intm_24_rt  ,
	nvl(nvl(b.intm_48_rt, c.intm_48_rt   ), d.intm_48_rt   )  as intm_48_rt   ,
	nvl(nvl(b.intm_72_rt, c.intm_72_rt  ), d.intm_72_rt  )  as intm_72_rt 
from tmp_dm_as.smc_sf_se_price_deal_use_tmp a 
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 where product_code = 'SE0118') b
	on a.src_code = b.src_dist_code 
	and a.dest_code = b.dest_dist_code
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_4 where product_code = 'SE0118') c
	on a.src_province = c.src_province
	and a.dest_province = c.dest_province
	and a.distance_section = c.distance_section
left join (select * from tmp_dm_as.ts_prod_od_tm_0701_tmp3_5 where product_code = 'SE0118') d 
	on a.distance_section = d.distance_section
;	


-------------------**********************----------------
---------------------仓发出的件时效计算-------------------------	
-------从仓发出的件时效计算 各流向均值，中位数，百分位数
drop table tmp_dm_as.whl_od_tm_shipment_tmp2_1;     
create table tmp_dm_as.whl_od_tm_shipment_tmp2_1 stored as parquet as
select 
	product_code,	
	src_dist_code,
	src_city_code,
	dest_dist_code,
	dest_city_code,
	avg(od_tm) as wh_avg_od_tm,
	count(*) as waybill_num       
from tmp_dm_as.whl_od_tm_shipment_tmp1
group by 
	product_code,	
	src_dist_code,
	src_city_code,
	dest_dist_code,
	dest_city_code  ;

--分位数
drop table tmp_dm_as.whl_od_tm_shipment_tmp2_2;  
create table tmp_dm_as.whl_od_tm_shipment_tmp2_2 stored as parquet as
select 
	product_code,	
	src_dist_code,
	src_city_code,
	dest_dist_code,
	dest_city_code,
	percentile_approx(od_tm,0.05,3000) as od_tm_05,	
	percentile_approx(od_tm,0.5,3000) as od_tm_mid,
	percentile_approx(od_tm,0.95,3000) as od_tm_95,
	percentile_approx(od_tm,0.25,3000) as od_tm_25,
	percentile_approx(od_tm,0.75,3000) as od_tm_75
from tmp_dm_as.whl_od_tm_shipment_tmp1
group by 
	product_code,	
	src_dist_code,
	src_city_code,
	dest_dist_code,
	dest_city_code;
 
--各流向众数
drop table tmp_dm_as.whl_od_tm_shipment_tmp2_3;    
create table tmp_dm_as.whl_od_tm_shipment_tmp2_3 stored as parquet as
select 
	product_code,	
	src_dist_code,
	src_city_code,
	dest_dist_code,
	dest_city_code,
	od_tm_hour as od_tm_mode
from 
	(select *,row_number() over(partition by product_code,src_dist_code,dest_dist_code order by od_tm_num desc) rn 
	from 
		(select
			product_code,	
			src_dist_code,
			src_city_code,
			dest_dist_code,
			dest_city_code,
			round(od_tm,0) as od_tm_hour,
			count(*) as od_tm_num
		from tmp_dm_as.whl_od_tm_shipment_tmp1
		group by 
			product_code,	
			src_dist_code,
			src_city_code,	
			dest_dist_code,
			dest_city_code,
			round(od_tm,0)  
	    ) a 
	) b 
where b.rn = 1 ;

----各流向达成率-24、48、72---------------------
--各流向时效达成
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp3_2;     
create table tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 stored as parquet as
select 
	product_code,
	src_dist_code,
	dest_dist_code,
	intm_24_num,
	intm_48_num,
	intm_72_num,
	tot_num,
	intm_24_num / tot_num as intm_24_rt,
	intm_48_num / tot_num as intm_48_rt,
	intm_72_num / tot_num as intm_72_rt
from 
	(select 
		product_code,	
		src_dist_code,
		dest_dist_code,
		sum(intm_24) as intm_24_num,
		sum(intm_48) as intm_48_num,	
		sum(intm_72) as intm_72_num,	
		count(*) as tot_num
	from tmp_dm_as.whl_od_tm_shipment_tmp1
	group by 
		product_code,	
		src_dist_code,
		dest_dist_code )t
where t.tot_num >= 10;

----------------------仓时效信息汇总--------------------
--组合  流向时效基础表
drop table tmp_dm_as.whl_od_tm_shipment_tm_res;      
create table tmp_dm_as.whl_od_tm_shipment_tm_res stored as parquet as
select 
	a.*,
	b.od_tm_05       as wh_od_tm_05     ,     	
	b.od_tm_mid      as wh_od_tm_mid    ,       
	b.od_tm_95       as wh_od_tm_95     ,
	c.od_tm_mode     as wh_od_tm_mode   ,
	b.od_tm_25       as wh_od_tm_25     ,
	b.od_tm_75       as wh_od_tm_75     ,
	d.intm_24_rt     as wh_intm_24_rt   ,
	d.intm_48_rt     as wh_intm_48_rt   ,
	d.intm_72_rt     as wh_intm_72_rt
from (select * from tmp_dm_as.whl_od_tm_shipment_tmp2_1 where waybill_num>=10)  a 
left join tmp_dm_as.whl_od_tm_shipment_tmp2_2 b 
	on a.product_code = b.product_code
	and a.src_dist_code = b.src_dist_code
	and a.dest_dist_code = b.dest_dist_code
left join tmp_dm_as.whl_od_tm_shipment_tmp2_3 c 
	on a.product_code = c.product_code
	and a.src_dist_code = c.src_dist_code
	and a.dest_dist_code = c.dest_dist_code
left join tmp_dm_as.ts_prod_od_tm_0701_tmp3_2 d 
	on a.product_code = d.product_code
	and a.src_dist_code = d.src_dist_code
	and a.dest_dist_code = d.dest_dist_code;



-------------------**********************----------------
------------------------时效汇总-------------------------
drop table tmp_dm_as.whl_ts_prod_od_tm_tmp;  
create table tmp_dm_as.whl_ts_prod_od_tm_tmp stored as parquet as	
select 
	a.src_code           ,
	a.src_city           ,
	a.src_province       ,
	a.src_dist           ,
	a.src_area           ,
	a.dest_code          ,
	a.dest_city          ,
	a.dest_province      ,
	a.dest_dist          ,
	a.dest_area          ,
	a.distance           ,
	a.distance_section   ,
	a.product_code       ,
	a.product_name       ,
	a.add_mark           ,
	round(a.avg_od_tm,2) as avg_od_tm ,
	round(a.od_tm_05 ,2) as od_tm_05  ,
	round(a.od_tm_mid,2) as od_tm_mid ,
	round(a.od_tm_95 ,2) as od_tm_95  ,
	round(a.od_tm_25 ,2) as od_tm_25  ,
	round(a.od_tm_75 ,2) as od_tm_75  ,
	a.od_tm_mode,
	b.intm_24_rt,
	b.intm_48_rt,
	b.intm_72_rt,
	round(c.wh_avg_od_tm,2)     as wh_avg_od_tm       ,
	round(c.wh_od_tm_05,2)        as wh_od_tm_05     ,
    round(c.wh_od_tm_mid,2)        as wh_od_tm_mid    ,
    round(c.wh_od_tm_95,2)       as wh_od_tm_95     ,
    c.wh_od_tm_mode   ,
    round(c.wh_od_tm_25,2)  as wh_od_tm_25     ,
    round(c.wh_od_tm_75,2)  as wh_od_tm_75 ,
    c.wh_intm_24_rt   ,
    c.wh_intm_48_rt   ,
    c.wh_intm_72_rt
from tmp_dm_as.whl_ts_prod_od_tm_detail a 
left join tmp_dm_as.whl_ts_prod_od_rt_detail2 b
	on a.src_code = b.src_code
	and a.dest_code = b.dest_code
	and a.product_code = b.product_code
left join tmp_dm_as.whl_od_tm_shipment_tm_res c 
	on a.src_code = c.src_dist_code
	and a.dest_code = c.dest_dist_code
	and a.product_code = c.product_code;


-------------------------------------------------------
--------------时效添加流向信息------------------------
---流向表
drop table tmp_dm_as.whl_ts_prod_od_liuxiang_tmp1;  
create table tmp_dm_as.whl_ts_prod_od_liuxiang_tmp1 stored as parquet as	
select 
	product_code,source_code,dest_code
from dm_pvs.tm_product_flow --新流向表
where inc_month = '$[day(yyyyMMdd,-1)]'
	and invalid_date is null
	and source_code is not null 
	and dest_code is not null
	and product_code in ('T2','SE0100','C2','SE0101')
    and flow_type = '1'
group by product_code,source_code,dest_code
union all
select 
	product_code,source_code,dest_code
from ods_pvs.tm_price_flow   --老流向表
where inc_month = '$[month(yyyyMM,-1)]'
	and invalid_date is null
	and source_code is not null 
	and dest_code is not null
	and product_code in ('S1','S2','C1','SE0020','SE0118','SE0117')	
    and outside = '0'  
group by product_code,source_code,dest_code	;   

---流向代码替换
drop table tmp_dm_as.whl_ts_prod_od_liuxiang_tmp2;  
create table tmp_dm_as.whl_ts_prod_od_liuxiang_tmp2 stored as parquet as
select 
	case when product_code = 'S1' then 'SE0001'
		 when product_code = 'T2' then 'SE0003'
		 when product_code = 'S2' then 'SE0004'
		 when product_code = 'C1' then 'SE0006'
		 when product_code = 'C2' then 'SE0021'
		else product_code end as product_code,
	source_code,
	dest_code
from tmp_dm_as.whl_ts_prod_od_liuxiang_tmp1 ;
 


--添加流向信息，判断该流向是否有价格数据
drop table tmp_dm_as.whl_ts_prod_od_tm;  
create table tmp_dm_as.whl_ts_prod_od_tm stored as parquet as	
select
	a.*,
	case when b.product_code is not null then 1 else 0 end as od_normal
from tmp_dm_as.whl_ts_prod_od_tm_tmp a     
left join tmp_dm_as.whl_ts_prod_od_liuxiang_tmp2 b 
	on a.product_code = b.product_code
	and a.src_code = b.source_code
	and a.dest_code = b.dest_code;


	

-------------起始点城市代码唯一，未将城市代码区分
---结果分区表  每月结果
/*
create table if not exists dm_as.ts_prod_city_code_od_tm
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
*/

insert overwrite table dm_as.ts_prod_city_code_od_tm  partition (inc_month = '$[month(yyyyMM,-1)]' )
select
	src_code           ,
	src_city           ,
	src_province       ,
	src_dist           ,
	src_area           ,
	dest_code          ,
	dest_city          ,
	dest_province      ,
	dest_dist          ,
	dest_area          ,
	distance           ,
	distance_section   ,
	product_code       ,
	product_name       ,
	add_mark           ,
	avg_od_tm ,
	od_tm_05  ,
	od_tm_mid ,
	od_tm_95  ,
	od_tm_mode,
	od_normal,
	od_tm_25,
	od_tm_75,
	intm_24_rt,
	intm_48_rt,
	intm_72_rt,
	wh_avg_od_tm   ,
	wh_od_tm_05    ,
	wh_od_tm_mid   ,
	wh_od_tm_95    ,
	wh_od_tm_mode  ,
	wh_od_tm_25    ,
	wh_od_tm_75    ,
	wh_intm_24_rt  ,
	wh_intm_48_rt  ,
	wh_intm_72_rt  
from tmp_dm_as.whl_ts_prod_od_tm;


-------------起始点城市唯一，已将城市代码区分-------------------------
drop table tmp_dm_as.whl_ts_prod_od_city_tm_tmp1;  
create table tmp_dm_as.whl_ts_prod_od_city_tm_tmp1 stored as parquet as	
select 
	src_code            ,
    deal_src_city as src_city            ,
    src_province        ,
    src_dist            ,
    src_area            ,
    dest_code           ,
    deal_dest_city as dest_city           ,
    dest_province       ,
	dest_dist          ,
	dest_area          ,
	distance           ,
	distance_section   ,
	product_code       ,
	product_name       ,
	add_mark           ,
	avg_od_tm ,
	od_tm_05  ,
	od_tm_mid ,
	od_tm_95  ,
	od_tm_mode,
	od_normal,
	od_tm_25,
	od_tm_75,
	intm_24_rt,
	intm_48_rt,
	intm_72_rt,
	wh_avg_od_tm   ,
	wh_od_tm_05    ,
	wh_od_tm_mid   ,
	wh_od_tm_95    ,
	wh_od_tm_mode  ,
	wh_od_tm_25    ,
	wh_od_tm_75    ,
	wh_intm_24_rt  ,
	wh_intm_48_rt  ,
	wh_intm_72_rt  
from 
	(select b.*,deal_src_city
	from 
		(select a.*,deal_dest_city
		from tmp_dm_as.whl_ts_prod_od_tm a 
		LATERAL VIEW explode(split(dest_city,'\\/')) cc AS deal_dest_city ) b 
	LATERAL VIEW explode(split(src_city,'\\/')) cc AS deal_src_city 
	)t ;

----------------------------------结果分区表  每月结果-----------------------
/*
create table if not exists dm_as.ts_prod_city_name_od_tm
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
*/

insert overwrite table dm_as.ts_prod_city_name_od_tm  partition (inc_month = '$[month(yyyyMM,-1)]')
select
	src_code           ,
	src_city           ,
	src_province       ,
	src_dist           ,
	src_area           ,
	dest_code          ,
	dest_city          ,
	dest_province      ,
	dest_dist          ,
	dest_area          ,
	distance           ,
	distance_section   ,
	product_code       ,
	product_name       ,
	add_mark           ,
	avg_od_tm ,
	od_tm_05  ,
	od_tm_mid ,
	od_tm_95  ,
	od_tm_mode,
	od_normal,
	od_tm_25,
	od_tm_75,
	intm_24_rt,
	intm_48_rt,
	intm_72_rt,
	wh_avg_od_tm   ,
	wh_od_tm_05    ,
	wh_od_tm_mid   ,
	wh_od_tm_95    ,
	wh_od_tm_mode  ,
	wh_od_tm_25    ,
	wh_od_tm_75    ,
	wh_intm_24_rt  ,
	wh_intm_48_rt  ,
	wh_intm_72_rt  
from tmp_dm_as.whl_ts_prod_od_city_tm_tmp1;



-------------------------******************************-----------------------------
-----匹配价格信息--------------------------------------------------
-----------------------------------------------------------------------
--挑选出城市到城市 并将对应的产品类型重新更新
drop table tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp0;  
create table tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp0 stored as parquet as	
select 
	*,
	case when product_code = 'S1' then 'SE0001'
		 when product_code = 'T2' then 'SE0003'
		 when product_code = 'S2' then 'SE0004'
		 when product_code = 'C1' then 'SE0006'
		 when product_code = 'C2' then 'SE0021'
		else product_code end as product_code_new        ---处理成时效中能对应的产品代码
from dm_as.joanna_price_base_all  
where inc_day = '$[day(yyyyMMdd,-1)]'
	and flow_type = '1'
	and product_code in ('S1','T2','S2','C1','C2','SE0100','SE0101','SE0020','SE0117','SE0118');
	
--产品代码匹配名称
drop table tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp1_1;  
create table tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp1_1 stored as parquet as	
select product_code_new,product_name
from tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp0 	
group by product_code_new,product_name;	
	
	
drop table tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp4;  
create table tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp4 stored as parquet as	
select 
	a.src_code           ,
    a.src_city           ,
    a.src_province       ,
    a.src_dist           ,
    a.src_area           ,
    a.dest_code          ,
    a.dest_city          ,
    a.dest_province      ,
    a.dest_dist          ,
    a.dest_area          ,
    a.distance           ,
    a.distance_section   ,
    a.product_code       ,   
	c.product_name,
    a.add_mark           ,
    a.avg_od_tm ,
    a.od_tm_05  ,
    a.od_tm_mid ,
    a.od_tm_95  ,
    a.od_tm_mode,
    a.od_normal,
	a.od_tm_25,
	a.od_tm_75,
	a.intm_24_rt,
	a.intm_48_rt,
	a.intm_72_rt,
	a.wh_avg_od_tm   ,
	a.wh_od_tm_05    ,
	a.wh_od_tm_mid   ,
	a.wh_od_tm_95    ,
	a.wh_od_tm_mode  ,
	a.wh_od_tm_25    ,
	a.wh_od_tm_75    ,
	a.wh_intm_24_rt  ,
	a.wh_intm_48_rt  ,
	a.wh_intm_72_rt  ,
	b.pay_country,
	b.price_code,
	b.base_weight_qty,
	b.base_price,
	b.weight_price_qty,
	b.max_weight_qty,
	b.light_factor
from 
	(select *
	from dm_as.ts_prod_city_code_od_tm
	where inc_month = '$[month(yyyyMM,-1)]') a 
left join tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp0 b 
on a.src_code = b.source_code
	and a.dest_code = b.dest_code
	and a.product_code = b.product_code_new
left join tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp1_1 c 
	on a.product_code = c.product_code_new;


	
	
	
----流向未拆分
-------------------------流向时效价格组合结果分区表  每月结果-----------------------
/*
create table if not exists dm_as.ts_prod_city_code_od_price_tm
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
*/

insert overwrite table dm_as.ts_prod_city_code_od_price_tm  partition (inc_month = '$[month(yyyyMM,-1)]' )
select
	src_code        ,
    src_city        ,
    src_province    ,
    src_dist        ,
    src_area        ,
    dest_code       ,
    dest_city       ,
    dest_province   ,
    dest_dist       ,
    dest_area       ,
    distance        ,
    distance_section,
    product_code    ,
    product_name    ,
    add_mark        ,
    avg_od_tm       ,
    od_tm_05        ,
    od_tm_mid       ,
    od_tm_95        ,
    od_tm_mode      ,
    pay_country     ,
    price_code      ,
    base_weight_qty ,
    base_price      ,
    weight_price_qty,
    max_weight_qty  ,
    light_factor    ,
    od_normal       ,
    od_tm_25        ,
    od_tm_75        ,
    intm_24_rt      ,
    intm_48_rt      ,
    intm_72_rt	    ,
	wh_avg_od_tm   ,
	wh_od_tm_05    ,
	wh_od_tm_mid   ,
	wh_od_tm_95    ,
	wh_od_tm_mode  ,
	wh_od_tm_25    ,
	wh_od_tm_75    ,
	wh_intm_24_rt  ,
	wh_intm_48_rt  ,
	wh_intm_72_rt  
from tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp4;

----------------------------流向拆分，用城市名称匹配--提供给北京团队的表-----------------------
drop table tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp5;  
create table tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp5 stored as parquet as	
select 
	src_code             ,
    deal_src_city as src_city             ,
    src_province         ,
    src_dist             ,
    src_area             ,
    dest_code            ,
    deal_dest_city as dest_city            ,
	dest_province      ,   
	dest_dist          ,
	dest_area          ,
	distance           ,
	distance_section   ,
	product_code       ,
	product_name       ,
	add_mark           ,
	avg_od_tm          ,
	od_tm_05           ,
	od_tm_mid          ,
	od_tm_95           ,
	od_tm_mode         ,
	pay_country        ,
	price_code         ,
	base_weight_qty    ,
	base_price         ,
	weight_price_qty   ,
	max_weight_qty     ,
	light_factor       ,
	od_normal          ,
	od_tm_25           ,
	od_tm_75           ,
	intm_24_rt         ,
	intm_48_rt         ,
	intm_72_rt	      ,
	wh_avg_od_tm   ,
	wh_od_tm_05    ,
	wh_od_tm_mid   ,
	wh_od_tm_95    ,
	wh_od_tm_mode  ,
	wh_od_tm_25    ,
	wh_od_tm_75    ,
	wh_intm_24_rt  ,
	wh_intm_48_rt  ,
	wh_intm_72_rt  
from 
	(select b.*,deal_src_city
	from 
		(select a.*,deal_dest_city
		from tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp4 a 
		LATERAL VIEW explode(split(dest_city,'\\/')) cc AS deal_dest_city ) b 
	LATERAL VIEW explode(split(src_city,'\\/')) cc AS deal_src_city 
	)t ;

/*
create table if not exists dm_as.ts_prod_city_name_od_price_tm
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
*/

insert overwrite table dm_as.ts_prod_city_name_od_price_tm  partition (inc_month = '$[month(yyyyMM,-1)]')
select
	src_code        ,
    src_city        ,
    src_province    ,
    src_dist        ,
    src_area        ,
    dest_code       ,
    dest_city       ,
    dest_province   ,
    dest_dist       ,
    dest_area       ,
    distance        ,
    distance_section,
    product_code    ,
    product_name    ,
    add_mark        ,
    avg_od_tm       ,
    od_tm_05        ,
    od_tm_mid       ,
    od_tm_95        ,
    od_tm_mode      ,
    pay_country     ,
    price_code      ,
    base_weight_qty ,
    base_price      ,
    weight_price_qty,
    max_weight_qty  ,
    light_factor    ,
    od_normal       ,
    od_tm_25        ,
    od_tm_75        ,
    intm_24_rt      ,
    intm_48_rt      ,
    intm_72_rt	   ,
	wh_avg_od_tm   ,
	wh_od_tm_05    ,
	wh_od_tm_mid   ,
	wh_od_tm_95    ,
	wh_od_tm_mode  ,
	wh_od_tm_25    ,
	wh_od_tm_75    ,
	wh_intm_24_rt  ,
	wh_intm_48_rt  ,
	wh_intm_72_rt  
from tmp_dm_as.whl_ts_prod_od_city_tm_price_tmp5;




/*
添加字段 dm_as.ts_prod_city_code_od_tm

--分区表可直接加字段，不影响历史数据
 ALTER TABLE dm_as.ts_prod_city_code_od_tm ADD COLUMNS 
	(od_tm_25           double   comment '25百分位时效',
	od_tm_75           double   comment '75百分位时效',
	intm_24_rt         double   comment '24小时达成率',
	intm_48_rt         double   comment '48小时达成率',
	intm_72_rt	       double   comment '72小时达成率');
 
 ALTER TABLE dm_as.ts_prod_city_name_od_tm ADD COLUMNS 
	(od_tm_25           double   comment '25百分位时效',
	od_tm_75           double   comment '75百分位时效',
	intm_24_rt         double   comment '24小时达成率',
	intm_48_rt         double   comment '48小时达成率',
	intm_72_rt	       double   comment '72小时达成率');

 ALTER TABLE dm_as.ts_prod_city_code_od_price_tm ADD COLUMNS 
	(od_normal          int      comment '该流向是否有对应产品的价格信息',
	od_tm_25           double   comment '25百分位时效',
	od_tm_75           double   comment '75百分位时效',
	intm_24_rt         double   comment '24小时达成率',
	intm_48_rt         double   comment '48小时达成率',
	intm_72_rt	       double   comment '72小时达成率');

 ALTER TABLE dm_as.ts_prod_city_name_od_price_tm ADD COLUMNS 
	(od_normal          int      comment '该流向是否有对应产品的价格信息',
	od_tm_25           double   comment '25百分位时效',
	od_tm_75           double   comment '75百分位时效',
	intm_24_rt         double   comment '24小时达成率',
	intm_48_rt         double   comment '48小时达成率',
	intm_72_rt	       double   comment '72小时达成率');


 **先删除分区再插入
ALTER TABLE dm_as.ts_prod_city_code_od_tm DROP IF EXISTS PARTITION (inc_month='201910');
ALTER TABLE dm_as.ts_prod_city_name_od_tm DROP IF EXISTS PARTITION (inc_month='201910');
ALTER TABLE dm_as.ts_prod_city_code_od_price_tm DROP IF EXISTS PARTITION (inc_month='201910');
ALTER TABLE dm_as.ts_prod_city_name_od_price_tm DROP IF EXISTS PARTITION (inc_month='201910');
*/


/*
添加字段    20191205 添加仓相关时效的信息
--分区表可直接加字段，不影响历史数据
 ALTER TABLE dm_as.ts_prod_city_code_od_tm ADD COLUMNS 
(  	     wh_avg_od_tm     double comment '顺丰仓发出-平均时效hour', 
		 wh_od_tm_05      double comment '顺丰仓发出-最快时效hour',
         wh_od_tm_mid     double comment '顺丰仓发出-中位数时效hour',
	     wh_od_tm_95      double comment '顺丰仓发出-最慢时效hour',
	     wh_od_tm_mode    double comment '顺丰仓发出-众数时效hour',
	     wh_od_tm_25      double comment '顺丰仓发出-25百分位时效',
	     wh_od_tm_75      double comment '顺丰仓发出-75百分位时效',
	     wh_intm_24_rt    double comment '顺丰仓发出-24小时达成率',
	     wh_intm_48_rt    double comment '顺丰仓发出-48小时达成率',
	     wh_intm_72_rt    double comment '顺丰仓发出-72小时达成率'
);
 
 ALTER TABLE dm_as.ts_prod_city_name_od_tm ADD COLUMNS 
(  	     wh_avg_od_tm     double comment '顺丰仓发出-平均时效hour', 
		 wh_od_tm_05      double comment '顺丰仓发出-最快时效hour',
         wh_od_tm_mid     double comment '顺丰仓发出-中位数时效hour',
	     wh_od_tm_95      double comment '顺丰仓发出-最慢时效hour',
	     wh_od_tm_mode    double comment '顺丰仓发出-众数时效hour',
	     wh_od_tm_25      double comment '顺丰仓发出-25百分位时效',
	     wh_od_tm_75      double comment '顺丰仓发出-75百分位时效',
	     wh_intm_24_rt    double comment '顺丰仓发出-24小时达成率',
	     wh_intm_48_rt    double comment '顺丰仓发出-48小时达成率',
	     wh_intm_72_rt    double comment '顺丰仓发出-72小时达成率'
);

 ALTER TABLE dm_as.ts_prod_city_code_od_price_tm ADD COLUMNS 
(  	     wh_avg_od_tm     double comment '顺丰仓发出-平均时效hour', 
		 wh_od_tm_05      double comment '顺丰仓发出-最快时效hour',
         wh_od_tm_mid     double comment '顺丰仓发出-中位数时效hour',
	     wh_od_tm_95      double comment '顺丰仓发出-最慢时效hour',
	     wh_od_tm_mode    double comment '顺丰仓发出-众数时效hour',
	     wh_od_tm_25      double comment '顺丰仓发出-25百分位时效',
	     wh_od_tm_75      double comment '顺丰仓发出-75百分位时效',
	     wh_intm_24_rt    double comment '顺丰仓发出-24小时达成率',
	     wh_intm_48_rt    double comment '顺丰仓发出-48小时达成率',
	     wh_intm_72_rt    double comment '顺丰仓发出-72小时达成率'
);

 ALTER TABLE dm_as.ts_prod_city_name_od_price_tm ADD COLUMNS 
(  	     wh_avg_od_tm     double comment '顺丰仓发出-平均时效hour', 
		 wh_od_tm_05      double comment '顺丰仓发出-最快时效hour',
         wh_od_tm_mid     double comment '顺丰仓发出-中位数时效hour',
	     wh_od_tm_95      double comment '顺丰仓发出-最慢时效hour',
	     wh_od_tm_mode    double comment '顺丰仓发出-众数时效hour',
	     wh_od_tm_25      double comment '顺丰仓发出-25百分位时效',
	     wh_od_tm_75      double comment '顺丰仓发出-75百分位时效',
	     wh_intm_24_rt    double comment '顺丰仓发出-24小时达成率',
	     wh_intm_48_rt    double comment '顺丰仓发出-48小时达成率',
	     wh_intm_72_rt    double comment '顺丰仓发出-72小时达成率'
);


 **先删除分区再插入
ALTER TABLE dm_as.ts_prod_city_code_od_tm DROP IF EXISTS PARTITION (inc_month='201911');
ALTER TABLE dm_as.ts_prod_city_name_od_tm DROP IF EXISTS PARTITION (inc_month='201911');
ALTER TABLE dm_as.ts_prod_city_code_od_price_tm DROP IF EXISTS PARTITION (inc_month='201911');
ALTER TABLE dm_as.ts_prod_city_name_od_price_tm DROP IF EXISTS PARTITION (inc_month='201911');
*/


/*

1.时效信息表名：dm_analysis.whl_ts_prod_od_tm，时效信息推荐使用[中位数时效]或[众数时效];
2.当前表中包含产品如下，通过product_code字段筛选
	SE0001	顺丰标快    S1
	SE0003	顺丰次晨    T2
	SE0004	顺丰特惠    S2
	SE0118	顺丰特惠B
	SE0117	顺丰特惠C
	SE0006	物流普运    C1
	SE0020	重货专运    SE0020
	SE0021	重货快运    C2 
	SE0100	重货包裹    SE0100
	SE0101	小票零担    SE0101
	
3.字段释义：
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
	add_mark           int      comment '是否补充数据，1是，0否',
	avg_od_tm          double   comment '平均时效hour',
	od_tm_05           double   comment '最快时效hour',
	od_tm_mid          double   comment '中位数时效hour',
	od_tm_95           double   comment '最慢时效hour',
	od_tm_mode         double   comment '众数时效hour'
	od_normal          string   comment '该流向是否有价格信息'
4.注意事项：由于原始流向表中存在少量一个城市代码对应多个城市名称，比如024对应沈阳市/铁岭市/抚顺市，所以尽量用中文市名匹配起点和终点

5.特殊情况，如SE0003顺丰次晨，	SE0021	重货快运
	SE0100	重货包裹   上海到新疆  具体4000-5000km，无历史数据，目前为空

*/



