set mapreduce.job.queuename=root.ISD;
set hive.exec.parallel=true;

-- 过敏
-- 目前问题：国际件存在多条矛盾；价格子表两条矛盾15-CNE1-D1（这个价格码是国际件的）；顺丰特惠(D)由于业务原因不能直接列出价格。

-- 产品名称/码，价格码，匹配新流向表（不包含锐特系统的医药零担，医药专车价格）
drop table tmp_dm_as.joanna_price_base_1;
create table tmp_dm_as.joanna_price_base_1 as
select 
	a.product_name,
	a.product_code,
	flow_type,
	source_code,
	dest_code,
	is_sale,
	pay_country,
	price_code,
	case when b1.product_code is not null then 'new' else 'old' end as data_type
from 
	(select *
	from 
		(select
		*,row_number() over(partition by product_code order by modified_tm desc) rn 
	from ods_pvs.tm_product_base_config
	where inc_day = '$[day(yyyyMMdd,-1)]'
		and (invalid_date is null or substr(invalid_date,1,10) > '$[day(yyyy-MM-dd,-1)]' ) ) t 
	where t.rn = 1 
	) a --中文名变成产品码
left join 
	(select * from 
		(select 
			flow_type,
			source_code,
			dest_code,
			pay_country,
			product_code,
			is_sale,
			price_code,
			row_number() over(partition by flow_type,source_code,dest_code,pay_country,product_code 
				order by modified_tm desc) rn
		from dm_pvs.tm_product_flow    --新流向表
		where inc_month = '$[day(yyyyMMdd,-1)]'
			and (invalid_date is null or substr(invalid_date,1,10) > '$[day(yyyy-MM-dd,-1)]' )
			and product_code not in ( 'S1','T1', 'SE0117', 'SE0118')   --新流向表中s1只有三条数据，取旧流向表数据,---特惠取旧流向表
			and is_sale = '0'
		) b11
	where rn = 1
	) b1 --产品码变成价格码，这里有不唯一的数据，开发去维护了
on a.product_code = b1.product_code;

	
-- 产品名称/码，价格码，匹配旧流向表（不包含锐特系统的医药零担，医药专车价格），不包含国际件价格
drop table tmp_dm_as.joanna_price_base_2;
create table tmp_dm_as.joanna_price_base_2 as
select 
	product_name,
	product_code,
	flow_type,
	source_code,
	dest_code,
	is_sale,
	pay_country,
	price_code,
	data_type
from tmp_dm_as.joanna_price_base_1
where data_type = 'new'
union all
select 
	a.product_name,
	a.product_code,
	b2.flow_type,
	b2.source_code,
	b2.dest_code,
	b2.is_sale,
	b2.pay_country,
	b2.price_code,
	a.data_type
from 
	(select * 
	from tmp_dm_as.joanna_price_base_1
	where data_type = 'old'
	) a
left join 
	(select * from 
		(select 
			case when outside = 0 then 1 else -1 end as flow_type,
			source_code,
			dest_code,
			pay_country,
			product_code,
			'0' as is_sale,
			price_code,
			row_number() over(partition by outside,source_code,dest_code,pay_country,product_code 
				order by modified_tm desc) rn
		from ods_pvs.tm_price_flow   --旧流向表
		where inc_month = '$[month(yyyyMM,-1)]'
			and (invalid_date is null or substr(invalid_date,1,10) > '$[day(yyyy-MM-dd,-1)]' )
			and dest_code is not null --首先去匹配dest_code，然后才是匹配dest_zone_id，匹配后者的一般都是国际件
		) b21
	where rn = 1
	) b2
on a.product_code = b2.product_code;
	
	
-- 现在国际件的价格区码配置数据等还有一些问题，所以这里先不看国际件的相关情况	
-- 做一个代码和名称匹配的表备用，pvs系统提供的表在匹配时问题有点多，所以还是用了zipper表
drop table tmp_dm_as.joanna_price_attach_name;
create table tmp_dm_as.joanna_price_attach_name as
select
	dist_code,
	city_name as city_name_split,
	provinct_name
from gdl.zipper_dim_department
where dw_end_date='99991231' and delete_flg<>1 and city_name is not null
group by dist_code,city_name,provinct_name;	
	
	
-- 流向名称匹配
drop table tmp_dm_as.joanna_price_base_3;
create table tmp_dm_as.joanna_price_base_3 as
select 
	product_name,
	product_code,
	flow_type,
	source_code,
	c.city_name_split as source_name,
	c.provinct_name as source_province,
	dest_code,
	d.city_name_split as dest_name,
	d.provinct_name as dest_province,
	is_sale,
	pay_country,
	price_code
from 
	tmp_dm_as.joanna_price_base_2 a
left join 
	tmp_dm_as.joanna_price_attach_name c 
on a.source_code = c.dist_code
left join 
	tmp_dm_as.joanna_price_attach_name d 
on a.dest_code = d.dist_code;

drop table tmp_dm_as.joanna_price_base_4_0;
create table tmp_dm_as.joanna_price_base_4_0 as
select * 
from 
	(select 
		*,row_number() over(partition by 
		price_code,calculate_code,size_max,light_factor,customer_flg order by modify_tm desc) rn
	from dm_pvs.tm_prod_price_rule --有重复记录，但内容是一致的
	where inc_month = '$[day(yyyyMMdd,-1)]' 
	) b1
where rn = 1;

-- 默认产品显示方案为-1 ，特惠B对应1，C对应2
--SE0117	顺丰特惠C
--SE0118	顺丰特惠B
drop table tmp_dm_as.joanna_price_base_4;
create table tmp_dm_as.joanna_price_base_4 as
select 
	a.*,
	c.base_weight_qty,
	c.base_price,
	c.weight_price_qty,
	c.max_weight_qty,
	b.light_factor
from 
	(select * 
	from tmp_dm_as.joanna_price_base_3
	where product_code not in ('SE0030','SE0117','SE0118','T6-L')      --'T6-L'特惠D暂不考虑 
	) a
left join tmp_dm_as.joanna_price_base_4_0 b --这里也有主键不唯一的数据要处理一下
on a.price_code = b.price_code
left join 
	(select * from 
		(select 
			price_code,
			plan,
			base_weight_qty,
			base_price,
			weight_price_qty,
			max_weight_qty,
			row_number() over(partition by price_code,base_weight_qty,base_price,weight_price_qty,max_weight_qty  
				order by modify_tm desc) rn    ----优先取2  去除可能存在的重复数据
			from 
				(select *
				from 
					(select *,row_number() over(partition by price_code,base_weight_qty	order by plan ) rd  
					from dm_pvs.tm_price_weight_1
					where inc_month = '$[day(yyyyMMdd,-1)]'   ) aa     --优先取-1
				) ss where rd = 1
		) c1
	where rn = 1
	) c --这里有不唯一的数据，开发去维护了
on a.price_code = c.price_code;

-- 特惠B对应1  SE0118	顺丰特惠B
drop table tmp_dm_as.joanna_price_base_5;
create table tmp_dm_as.joanna_price_base_5 as
select 
	a.*,
	c.base_weight_qty,
	c.base_price,
	c.weight_price_qty,
	c.max_weight_qty,
	b.light_factor
from 
	(select * 
	from tmp_dm_as.joanna_price_base_3
	where product_code = 'SE0118'      --'T6-L'特惠D暂不考虑 
	) a
left join tmp_dm_as.joanna_price_base_4_0 b --这里也有主键不唯一的数据要处理一下
on a.price_code = b.price_code
left join 
	(select * from 
		(select 
			price_code,
			plan,
			base_weight_qty,
			base_price,
			weight_price_qty,
			max_weight_qty,
			row_number() over(partition by price_code,base_weight_qty,base_price,weight_price_qty,max_weight_qty  
				order by modify_tm desc) rn    ----优先取2  去除可能存在的重复数据
			from 
				(select *
				from 
					(select *,row_number() over(partition by price_code,base_weight_qty	order by plan ) rd  
					from dm_pvs.tm_price_weight_1
					where inc_month = '$[day(yyyyMMdd,-1)]'   
						and plan in ('-1','1')  ) aa     --优先取1
				) ss where rd = 1
		) c1
	where rn = 1
	) c --这里有不唯一的数据，开发去维护了
on a.price_code = c.price_code;


--C对应2  --SE0117	顺丰特惠C  
drop table tmp_dm_as.joanna_price_base_6;
create table tmp_dm_as.joanna_price_base_6 as
select 
	a.*,
	c.base_weight_qty,
	c.base_price,
	c.weight_price_qty,
	c.max_weight_qty,
	b.light_factor
from 
	(select * 
	from tmp_dm_as.joanna_price_base_3
	where product_code = 'SE0117'      --'T6-L'特惠D暂不考虑 
	) a
left join tmp_dm_as.joanna_price_base_4_0 b --这里也有主键不唯一的数据要处理一下
on a.price_code = b.price_code
left join 
	(select * from 
		(select 
			price_code,
			plan,
			base_weight_qty,
			base_price,
			weight_price_qty,
			max_weight_qty,
			row_number() over(partition by price_code,base_weight_qty,base_price,weight_price_qty,max_weight_qty  
				order by modify_tm desc) rn    ----优先取2  去除可能存在的重复数据
			from 
				(select *
				from 
					(select *,row_number() over(partition by price_code,base_weight_qty	order by plan ) rd  
					from dm_pvs.tm_price_weight_1
					where inc_month = '$[day(yyyyMMdd,-1)]'   
						and plan in ('-1','2')  ) aa     --优先取2
				) ss where rd = 1
		) c1
	where rn = 1
	) c --这里有不唯一的数据，开发去维护了
on a.price_code = c.price_code;

--冷运零担 SE0030 使用ods_pvs.tm_price_weight_2 的价格区码数据
--注意！！！该计费方式为最低收费和单位重量计费取较大的值，不是首重续重的计费方式
drop table tmp_dm_as.joanna_price_base_7;
create table tmp_dm_as.joanna_price_base_7 as
select 
	a.*,
	c.base_weight_qty,
	c.base_price,
	c.weight_price_qty,
	c.max_weight_qty,
	b.light_factor
from 
	(select * 
	from tmp_dm_as.joanna_price_base_3
	where product_code = 'SE0030'      
	) a
left join tmp_dm_as.joanna_price_base_4_0 b --这里也有主键不唯一的数据要处理一下
on a.price_code = b.price_code
left join 
	(select * from 
		(select 
			price_code,
			weight_min as base_weight_qty,
			min_fee as base_price,
			unit_price as weight_price_qty,
			weight_max as max_weight_qty,
			row_number() over(partition by price_code,weight_min,min_fee,unit_price,weight_max  
				order by modifier_tm desc) rn    ----优先取2  去除可能存在的重复数据
			from 
				(select *
				from 
					(select *,row_number() over(partition by price_code,weight_min	order by modifier_tm ) rd  
					from ods_pvs.tm_price_weight_2
					where inc_day = '$[day(yyyyMMdd,-1)]'   ) aa   
				) ss where rd = 1
		) c1
	where rn = 1
	) c --这里有不唯一的数据，开发去维护了
on a.price_code = c.price_code;


-- 顺丰特惠(D)，得确定客户签的到底是ABC哪个方案，所以这个产品没有做表
-- 汇总
drop table tmp_dm_as.joanna_price_base_all;
create table tmp_dm_as.joanna_price_base_all as
select 
	product_name,
	product_code,
	flow_type,
	source_code,
	source_name,
	source_province,
	dest_code,
	dest_name,
	dest_province,
	is_sale,
	pay_country,
	price_code,
	base_weight_qty,
	base_price,
	weight_price_qty,
	max_weight_qty,
	light_factor
from tmp_dm_as.joanna_price_base_4
union all
select
	product_name,
	product_code,
	flow_type,
	source_code,
	source_name,
	source_province,
	dest_code,
	dest_name,
	dest_province,
	is_sale,
	pay_country,
	price_code,
	base_weight_qty,
	base_price,
	weight_price_qty,
	max_weight_qty,
	light_factor
from tmp_dm_as.joanna_price_base_5
union all
select
	product_name,
	product_code,
	flow_type,
	source_code,
	source_name,
	source_province,
	dest_code,
	dest_name,
	dest_province,
	is_sale,
	pay_country,
	price_code,
	base_weight_qty,
	base_price,
	weight_price_qty,
	max_weight_qty,
	light_factor
from tmp_dm_as.joanna_price_base_6
union all
select
	product_name,
	product_code,
	flow_type,
	source_code,
	source_name,
	source_province,
	dest_code,
	dest_name,
	dest_province,
	is_sale,
	pay_country,
	price_code,
	base_weight_qty,
	base_price,
	weight_price_qty,
	max_weight_qty,
	light_factor
from tmp_dm_as.joanna_price_base_7;

/*
create table if not exists `dm_as.joanna_price_base_all` 
    (
	product_name	string	comment '产品名称'	,
	product_code	string	comment '产品代码'	,
	flow_type	int	comment '流向类型：-1：城市-城市（郊区件）；1：城市-城市；2：行政区-行政区；3：行政区-城市；4：网点-网点；5：网点-城市；6：专业市场-城市；7：城市-城市群；8：城市群-城市群；9：国家-国家'	,
	source_code	string	comment '原寄地'	,
	source_name	string	comment '原寄地城市名称'	,
	source_province	string	comment '原寄地省份名称'	,
	dest_code	string	comment '目的地'	,
	dest_name	string	comment '目的地城市名称'	,
	dest_province	string	comment '目的地省份名称'	,
	is_sale	int	comment '是否促销(0:否,1:是)'	,
	pay_country	string	comment '付款地国家'	,
	price_code	string	comment '价格区码'	,
	base_weight_qty	string	comment '首重'	,
	base_price	string	comment '首重运费'	,
	weight_price_qty	string	comment '续重运费'	,
	max_weight_qty	string	comment '最大重量'	,
	light_factor	string	comment '轻抛'
	)
    partitioned by (`inc_day` string) stored as parquet;
*/
	
insert overwrite table dm_as.joanna_price_base_all partition (inc_day = '$[day(yyyyMMdd,-1)]') 
select
	product_name,
	product_code,
	flow_type,
	source_code,
	source_name,
	source_province,
	dest_code,
	dest_name,
	dest_province,
	'0' as is_sale,
	pay_country,
	price_code,
	base_weight_qty,
	base_price,
	weight_price_qty,
	max_weight_qty,
	light_factor
from tmp_dm_as.joanna_price_base_all;
