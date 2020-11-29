set mapreduce.job.queuename=root.ISD;
---------时效调度-----------------------------------------------------
--------------------------------------------------------------------------------------------------------------
--时效数据
--*******************************************************--
-------------------------运单基础信息--------------------------
--*******************************************************--
--流向基础数据
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp1_1;  
create table tmp_dm_as.ts_prod_od_tm_0701_tmp1_1 stored as parquet as	
select
	t.*,
	(unix_timestamp(signin_tm) - unix_timestamp(consigned_tm)) / 3600 as od_tm_org
from 
	(select 
		waybill_no,
		source_zone_code,
		dest_zone_code,
		src_dist_code,   --755
		src_city_code,    --ShenZhen
		dest_dist_code,
		dest_city_code,
		product_code,
		freight_monthly_acct_code,
		cod_monthly_acct_code,
		nvl(substr(recv_bar_tm,1,19),consigned_tm) as consigned_tm,
		nvl(substr(send_bar_tm,1,19),signin_tm)  as signin_tm,
		inc_day
	from gdl.tt_waybill_info
	where inc_day between '$[day(yyyyMMdd,-92)]' and '$[day(yyyyMMdd,-1)]'
			and (nvl(substr(recv_bar_tm,1,19),consigned_tm) between '$[day(yyyy-MM-dd,-92)]' and '$[day(yyyy-MM-dd,-1)]') 
      and last_abnormal_tm is null   --0816新增派  最后派件异常时间  存在异常的件不在考虑范围内
	  and product_code in ('SE0001','SE0003','SE0004','SE0118','SE0117','SE0006','SE0020','SE0021','SE0100','SE0101')
	group by 
		waybill_no,
		source_zone_code,
		dest_zone_code,
		src_dist_code,   
		src_city_code,  
		dest_dist_code,
		dest_city_code,
		product_code,
		freight_monthly_acct_code,
		cod_monthly_acct_code,
		nvl(substr(recv_bar_tm,1,19),consigned_tm),
		nvl(substr(send_bar_tm,1,19),signin_tm),
		inc_day
	) t ;

	
--11月份时效修改	
--and ((nvl(substr(recv_bar_tm,1,19),consigned_tm) between '$[day(yyyy-MM-dd,-120)]' and '2019-10-31')  
--or (nvl(substr(recv_bar_tm,1,19),consigned_tm) between '2019-11-30'	and '$[day(yyyy-MM-dd,-1)]') )

---去除特惠的转寄退回件，影响同城时效计算-- 20191127修改逻辑：标快，特惠系列同城件中，起点终点相同且时效超过48小时的运单删除，超过48小时判定为异常订单
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp1_2;  
create table tmp_dm_as.ts_prod_od_tm_0701_tmp1_2 stored as parquet as	
select a.*
from 
	(select *,
		case when source_zone_code = dest_zone_code and product_code in ('SE0004','SE0118','SE0117','SE0001','SE0003') and od_tm_org>=48 then 1 else 0 end as outlier
	from tmp_dm_as.ts_prod_od_tm_0701_tmp1_1 
	where src_dist_code is not null and dest_dist_code is not null and product_code is not null) a
where outlier = 0 ;

-------------------***********-------------------------
-------------------关联FVP数据-------------------------
-------------------------------------------------------
-------------对取出的件做去重操作---数据来源：191996  时效表优化FVP数据
--1.去除有转寄退回操作的件
drop table if exists tmp_dm_as.whl_od_tm_fvp_99_tmp2_1;
create table if not exists tmp_dm_as.whl_od_tm_fvp_99_tmp2_1 stored as parquet as 
select 
	mainwaybillno
from dm_as.whl_od_tm_fvp_125
where inc_day between '$[day(yyyyMMdd,-92)]' and '$[day(yyyyMMdd,-1)]'
	and to_date(barscantm) between '$[day(yyyy-MM-dd,-92)]' and '$[day(yyyy-MM-dd,-1)]'
	and opcode in( '99','870');
		

drop table if exists tmp_dm_as.whl_od_tm_fvp_125_tmp2_2;
create table if not exists tmp_dm_as.whl_od_tm_fvp_125_tmp2_2 stored as parquet as 
select
	a.*
from tmp_dm_as.ts_prod_od_tm_0701_tmp1_2 a 
left join tmp_dm_as.whl_od_tm_fvp_99_tmp2_1 b 
	on a.waybill_no = b.mainwaybillno
where b.mainwaybillno is null;

--2.选择放入丰巢柜的时间,或交接给合作点的时间
drop table if exists tmp_dm_as.whl_od_tm_fvp_125_tmp2;
create table if not exists tmp_dm_as.whl_od_tm_fvp_125_tmp2 stored as parquet as 
select 
	t.*
from 
	(select 
		*,row_number() over(partition by mainwaybillno order by barscantm) rn
	from dm_as.whl_od_tm_fvp_125
	where inc_day between '$[day(yyyyMMdd,-92)]' and '$[day(yyyyMMdd,-0)]'
		and to_date(barscantm) between '$[day(yyyy-MM-dd,-92)]' and '$[day(yyyy-MM-dd,-1)]'
		and opcode in ( '125','657')
	) t 
where t.rn = 1;

--关联存在125操作的件，若125时间早于收件时间，则取125时间作为到达时间
drop table if exists tmp_dm_as.whl_od_tm_fvp_125_tmp3;
create table if not exists tmp_dm_as.whl_od_tm_fvp_125_tmp3 stored as parquet as 
select
	a.*,b.barscantm,
	case when b.barscantm is not null and unix_timestamp(b.barscantm) < unix_timestamp(a.signin_tm) then b.barscantm else a.signin_tm end as arrival_tm
from tmp_dm_as.whl_od_tm_fvp_125_tmp2_2 a 
left join tmp_dm_as.whl_od_tm_fvp_125_tmp2 b 
	on a.waybill_no = b.mainwaybillno;

	
----------**********************************------------------
---------时效数据基表--------------
drop table tmp_dm_as.ts_prod_od_tm_0701_tmp1;  
create table tmp_dm_as.ts_prod_od_tm_0701_tmp1 stored as parquet as	
select t.*,
		case when od_tm<=24 then 1 else 0 end as intm_24,
		case when od_tm<=48 then 1 else 0 end as intm_48,
		case when od_tm<=72 then 1 else 0 end as intm_72 
from 
	(select *,
		(unix_timestamp(arrival_tm) - unix_timestamp(consigned_tm)) / 3600 as od_tm
	from tmp_dm_as.whl_od_tm_fvp_125_tmp3 ) t  
where od_tm>0;


 
	
----------------**************************---------------------------
------------------------仓库运单数据,同时去除重复项--------------------------------
drop table if exists tmp_dm_as.whl_od_tm_shipment_tmp;
create table if not exists tmp_dm_as.whl_od_tm_shipment_tmp stored as parquet as 
select 
	t.*
from 
	(select
		waybill_no, 
		warehouse_code,
		warehouse_name,
		carrier_serviece_name,
		SUBSTRING(shipment_date,0,10) as actual_shipment_date,
		row_number() over(partition by waybill_no order by shipment_date)rn
	FROM gdl.mm_shipment_item_info
	where inc_day between '$[day(yyyyMMdd,-92)]' and '$[day(yyyyMMdd,-1)]'
			and SUBSTRING(shipment_date,0,10)  between '$[day(yyyy-MM-dd,-92)]' and '$[day(yyyy-MM-dd,-1)]' )  t 
where t.rn = 1;


--关联运单表，作为从仓发货的基础数据
drop table if exists tmp_dm_as.whl_od_tm_shipment_tmp1;
create table if not exists tmp_dm_as.whl_od_tm_shipment_tmp1 stored as parquet as 
select
	a.*,
	b.warehouse_code,
	b.warehouse_name,
	b.carrier_serviece_name,
	b.actual_shipment_date
from tmp_dm_as.ts_prod_od_tm_0701_tmp1 a 
left join tmp_dm_as.whl_od_tm_shipment_tmp b 
	on a.waybill_no = b.waybill_no
where warehouse_code is not null ;
