--参数配置
set hive.execution.engine=tez;

--时效合并
--限定至少有10单，才能有效
drop table tmp_dm_as.ts_supply_way_tm_flowbase_tmp;
create table tmp_dm_as.ts_supply_way_tm_flowbase_tmp stored as parquet as
select a.flow_product_code
      ,a.src_dist_code
      ,a.src_city_code
      ,a.dest_dist_code
      ,a.dest_city_code
      ,a.waybill_num
      ,a.avg_od_tm
      ,a.od_tm_05
      ,a.od_tm_25
      ,a.od_tm_40
      ,a.od_tm_45
      ,a.od_tm_mid
      ,a.od_tm_55
      ,a.od_tm_60
      ,a.od_tm_65
      ,a.od_tm_70
      ,a.od_tm_75
      ,a.od_tm_80
      ,a.od_tm_85
      ,a.od_tm_90
      ,a.od_tm_95
      ,a.intm_24_num
      ,a.intm_36_num
      ,a.intm_42_num
      ,a.intm_48_num
      ,a.intm_54_num
      ,a.intm_60_num
      ,a.intm_66_num
      ,a.intm_72_num
      ,a.intm_78_num
      ,a.intm_84_num
      ,a.intm_90_num
      ,a.intm_96_num
      ,a.intm_2D12_num
      ,a.intm_2D18_num
      ,a.intm_3D12_num
      ,a.intm_3D18_num
      ,a.intm_4D12_num
      ,a.intm_4D18_num
      ,a.intm_that_num
      ,a.intm_next_num
      ,a.intm_other_num
      ,b.od_tm_mode
  from 
       (select *
          from tmp_dm_as.ts_supply_way_tm_avg_percentile_tmp  a  --物流产品时效分位数计算
          where waybill_num>=10
       ) a

       join

       tmp_dm_as.ts_supply_way_tm_mode_tmp b   --物流产品时效众数计算
       on a.flow_product_code=b.flow_product_code and a.src_dist_code = b.src_dist_code and a.dest_dist_code = b.dest_dist_code;

---------------------------------------------------------------------------
--补充数据准备
--关联流向全表，获得省，距离相关数据
--编码也关联得到价格编码，也就是price_product_code
drop table tmp_dm_as.ts_supply_way_tm_flowbase_all_tmp;  
create table tmp_dm_as.ts_supply_way_tm_flowbase_all_tmp stored as parquet as
select a.price_product_code product_code       --价格的代码为唯一代码
      ,a.price_product_name product_name 
      ,a.flow_product_code
      ,a.src_code,a.src_city,a.src_province,a.src_dist,a.src_area,a.dest_code,a.dest_city,a.dest_province,a.dest_dist,a.dest_area,a.area_type,a.distance,a.distance_section
      ,b.waybill_num,b.avg_od_tm,b.od_tm_mode,b.od_tm_05,b.od_tm_25,b.od_tm_40,b.od_tm_45,b.od_tm_mid,b.od_tm_55,b.od_tm_60,b.od_tm_65,b.od_tm_70,b.od_tm_75,b.od_tm_80,b.od_tm_85,b.od_tm_90,b.od_tm_95
      ,case when b.waybill_num>0 then b.intm_24_num*1.0/b.waybill_num else cast(null as double) end as intm_24_rt
      ,case when b.waybill_num>0 then b.intm_36_num*1.0/b.waybill_num else cast(null as double) end as intm_36_rt
      ,case when b.waybill_num>0 then b.intm_42_num*1.0/b.waybill_num else cast(null as double) end as intm_42_rt
      ,case when b.waybill_num>0 then b.intm_48_num*1.0/b.waybill_num else cast(null as double) end as intm_48_rt
      ,case when b.waybill_num>0 then b.intm_54_num*1.0/b.waybill_num else cast(null as double) end as intm_54_rt
      ,case when b.waybill_num>0 then b.intm_60_num*1.0/b.waybill_num else cast(null as double) end as intm_60_rt
      ,case when b.waybill_num>0 then b.intm_66_num*1.0/b.waybill_num else cast(null as double) end as intm_66_rt
      ,case when b.waybill_num>0 then b.intm_72_num*1.0/b.waybill_num else cast(null as double) end as intm_72_rt
      ,case when b.waybill_num>0 then b.intm_78_num*1.0/b.waybill_num else cast(null as double) end as intm_78_rt
      ,case when b.waybill_num>0 then b.intm_84_num*1.0/b.waybill_num else cast(null as double) end as intm_84_rt
      ,case when b.waybill_num>0 then b.intm_90_num*1.0/b.waybill_num else cast(null as double) end as intm_90_rt
      ,case when b.waybill_num>0 then b.intm_96_num*1.0/b.waybill_num else cast(null as double) end as intm_96_rt
      ,case when b.waybill_num>0 then b.intm_2D12_num*1.0/b.waybill_num else cast(null as double) end as intm_2D12_rt
      ,case when b.waybill_num>0 then b.intm_2D18_num*1.0/b.waybill_num else cast(null as double) end as intm_2D18_rt
      ,case when b.waybill_num>0 then b.intm_3D12_num*1.0/b.waybill_num else cast(null as double) end as intm_3D12_rt
      ,case when b.waybill_num>0 then b.intm_3D18_num*1.0/b.waybill_num else cast(null as double) end as intm_3D18_rt
      ,case when b.waybill_num>0 then b.intm_4D12_num*1.0/b.waybill_num else cast(null as double) end as intm_4D12_rt
      ,case when b.waybill_num>0 then b.intm_4D18_num*1.0/b.waybill_num else cast(null as double) end as intm_4D18_rt
      ,case when b.waybill_num>0 then b.intm_that_num*1.0/b.waybill_num else cast(null as double) end as intm_that_rt
      ,case when b.waybill_num>0 then b.intm_next_num*1.0/b.waybill_num else cast(null as double) end as intm_next_rt
      ,case when b.waybill_num>0 then b.intm_other_num*1.0/b.waybill_num else cast(null as double) end as intm_other_rt
  from 
       dm_as.ts_supply_product_flow_all_info a       ---基础流向表
     
       left join 

       tmp_dm_as.ts_supply_way_tm_flowbase_tmp b on a.src_code = b.src_dist_code and a.dest_code = b.dest_dist_code and a.flow_product_code=b.flow_product_code;

--对于没有时效数据的流向，用产品+省份+距离区间维度的均值来替代，如果此均值也没有，则用产品+距离区间均值替代
--下面开始填补
--填补数据准备
--各产品+省+距离均值
drop table tmp_dm_as.ts_supply_way_tm_flow_province_tmp;  
create table tmp_dm_as.ts_supply_way_tm_flow_province_tmp stored as parquet as
select product_code 
      ,src_province
      ,dest_province
      ,distance_section
      ,avg(avg_od_tm) as avg_od_tm
      ,avg(od_tm_mode) as od_tm_mode
      ,avg(od_tm_05) as od_tm_05
      ,avg(od_tm_25) as od_tm_25
      ,avg(od_tm_40) as od_tm_40
      ,avg(od_tm_45) as od_tm_45
      ,avg(od_tm_mid) as od_tm_mid
      ,avg(od_tm_55) as od_tm_55
      ,avg(od_tm_60) as od_tm_60
      ,avg(od_tm_65) as od_tm_65
      ,avg(od_tm_70) as od_tm_70
      ,avg(od_tm_75) as od_tm_75
      ,avg(od_tm_80) as od_tm_80
      ,avg(od_tm_85) as od_tm_85
      ,avg(od_tm_90) as od_tm_90
      ,avg(od_tm_95) as od_tm_95
      ,sum(intm_24_rt*waybill_num)*1.0/sum(waybill_num) as intm_24_rt 
      ,sum(intm_36_rt*waybill_num)*1.0/sum(waybill_num) as intm_36_rt 
      ,sum(intm_42_rt*waybill_num)*1.0/sum(waybill_num) as intm_42_rt 
      ,sum(intm_48_rt*waybill_num)*1.0/sum(waybill_num) as intm_48_rt 
      ,sum(intm_54_rt*waybill_num)*1.0/sum(waybill_num) as intm_54_rt 
      ,sum(intm_60_rt*waybill_num)*1.0/sum(waybill_num) as intm_60_rt 
      ,sum(intm_66_rt*waybill_num)*1.0/sum(waybill_num) as intm_66_rt 
      ,sum(intm_72_rt*waybill_num)*1.0/sum(waybill_num) as intm_72_rt 
      ,sum(intm_78_rt*waybill_num)*1.0/sum(waybill_num) as intm_78_rt 
      ,sum(intm_84_rt*waybill_num)*1.0/sum(waybill_num) as intm_84_rt 
      ,sum(intm_90_rt*waybill_num)*1.0/sum(waybill_num) as intm_90_rt 
      ,sum(intm_96_rt*waybill_num)*1.0/sum(waybill_num) as intm_96_rt 
      ,sum(intm_2D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_2D12_rt 
      ,sum(intm_2D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_2D18_rt 
      ,sum(intm_3D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_3D12_rt 
      ,sum(intm_3D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_3D18_rt 
      ,sum(intm_4D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_4D12_rt 
      ,sum(intm_4D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_4D18_rt 
      ,sum(intm_that_rt*waybill_num)*1.0/sum(waybill_num) as intm_that_rt 
      ,sum(intm_next_rt*waybill_num)*1.0/sum(waybill_num) as intm_next_rt 
      ,sum(intm_other_rt*waybill_num)*1.0/sum(waybill_num) as intm_other_rt 
      ,sum(waybill_num) as waybill_num
  from tmp_dm_as.ts_supply_way_tm_flowbase_all_tmp
 where waybill_num>=10
 group by product_code,src_province,dest_province,distance_section;

--各产品+距离均值
--大票零担数据太少，距离较长的几个流向，用特惠的1.6倍替代，大票零担不承诺时效
--重货专运没有2500~3000的数据，用2000~2500替代，重货专运也不承诺时效
--3500以上的距离要填补
--上面三种情况单拎出来处理
drop table tmp_dm_as.ts_supply_way_tm_flow_distance_pure_tmp;  
create table tmp_dm_as.ts_supply_way_tm_flow_distance_pure_tmp as
select product_code
      ,distance_section
      ,avg(avg_od_tm) as avg_od_tm
      ,avg(od_tm_mode) as od_tm_mode
      ,avg(od_tm_05) as od_tm_05
      ,avg(od_tm_25) as od_tm_25
      ,avg(od_tm_40) as od_tm_40
      ,avg(od_tm_45) as od_tm_45
      ,avg(od_tm_mid) as od_tm_mid
      ,avg(od_tm_55) as od_tm_55
      ,avg(od_tm_60) as od_tm_60
      ,avg(od_tm_65) as od_tm_65
      ,avg(od_tm_70) as od_tm_70
      ,avg(od_tm_75) as od_tm_75
      ,avg(od_tm_80) as od_tm_80
      ,avg(od_tm_85) as od_tm_85
      ,avg(od_tm_90) as od_tm_90
      ,avg(od_tm_95) as od_tm_95
      ,sum(intm_24_rt*waybill_num)*1.0/sum(waybill_num) as intm_24_rt 
      ,sum(intm_36_rt*waybill_num)*1.0/sum(waybill_num) as intm_36_rt 
      ,sum(intm_42_rt*waybill_num)*1.0/sum(waybill_num) as intm_42_rt 
      ,sum(intm_48_rt*waybill_num)*1.0/sum(waybill_num) as intm_48_rt 
      ,sum(intm_54_rt*waybill_num)*1.0/sum(waybill_num) as intm_54_rt 
      ,sum(intm_60_rt*waybill_num)*1.0/sum(waybill_num) as intm_60_rt 
      ,sum(intm_66_rt*waybill_num)*1.0/sum(waybill_num) as intm_66_rt 
      ,sum(intm_72_rt*waybill_num)*1.0/sum(waybill_num) as intm_72_rt 
      ,sum(intm_78_rt*waybill_num)*1.0/sum(waybill_num) as intm_78_rt 
      ,sum(intm_84_rt*waybill_num)*1.0/sum(waybill_num) as intm_84_rt 
      ,sum(intm_90_rt*waybill_num)*1.0/sum(waybill_num) as intm_90_rt 
      ,sum(intm_96_rt*waybill_num)*1.0/sum(waybill_num) as intm_96_rt 
      ,sum(intm_2D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_2D12_rt 
      ,sum(intm_2D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_2D18_rt 
      ,sum(intm_3D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_3D12_rt 
      ,sum(intm_3D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_3D18_rt 
      ,sum(intm_4D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_4D12_rt 
      ,sum(intm_4D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_4D18_rt 
      ,sum(intm_that_rt*waybill_num)*1.0/sum(waybill_num) as intm_that_rt 
      ,sum(intm_next_rt*waybill_num)*1.0/sum(waybill_num) as intm_next_rt 
      ,sum(intm_other_rt*waybill_num)*1.0/sum(waybill_num) as intm_other_rt 
      ,sum(waybill_num) waybill_num
  from tmp_dm_as.ts_supply_way_tm_flowbase_all_tmp
 where waybill_num>=10 
   and ((product_code='SE0114' and distance_section not in ('(2500-3000]','(3000-3500]','(3500-4000]'))
         or (product_code='SE0020' and distance_section not in ('(2500-3000]'))
         or product_code not in ('SE0114','SE0020'))  --SE0114大票零担，SE0020重货专运
 group by product_code,distance_section;
  
--大票零担距离较长的三个区间时效用特惠的1.6倍替代
insert into table tmp_dm_as.ts_supply_way_tm_flow_distance_pure_tmp
select 'SE0114' product_code
      ,distance_section
      ,avg(avg_od_tm)*1.6 as avg_od_tm
      ,avg(od_tm_mode)*1.6 as od_tm_mode
      ,avg(od_tm_05)*1.6 as od_tm_05
      ,avg(od_tm_25)*1.6 as od_tm_25
      ,avg(od_tm_40)*1.6 as od_tm_40
      ,avg(od_tm_45)*1.6 as od_tm_45
      ,avg(od_tm_mid)*1.6 as od_tm_mid
      ,avg(od_tm_55)*1.6 as od_tm_55
      ,avg(od_tm_60)*1.6 as od_tm_60
      ,avg(od_tm_65)*1.6 as od_tm_65
      ,avg(od_tm_70)*1.6 as od_tm_70
      ,avg(od_tm_75)*1.6 as od_tm_75
      ,avg(od_tm_80)*1.6 as od_tm_80
      ,avg(od_tm_85)*1.6 as od_tm_85
      ,avg(od_tm_90)*1.6 as od_tm_90
      ,avg(od_tm_95)*1.6 as od_tm_95
      ,0.5*sum(intm_24_rt*waybill_num)*1.0/sum(waybill_num) as intm_24_rt 
      ,0.5*sum(intm_36_rt*waybill_num)*1.0/sum(waybill_num) as intm_36_rt 
      ,0.5*sum(intm_42_rt*waybill_num)*1.0/sum(waybill_num) as intm_42_rt 
      ,0.5*sum(intm_48_rt*waybill_num)*1.0/sum(waybill_num) as intm_48_rt 
      ,0.5*sum(intm_54_rt*waybill_num)*1.0/sum(waybill_num) as intm_54_rt 
      ,0.5*sum(intm_60_rt*waybill_num)*1.0/sum(waybill_num) as intm_60_rt 
      ,0.5*sum(intm_66_rt*waybill_num)*1.0/sum(waybill_num) as intm_66_rt 
      ,0.5*sum(intm_72_rt*waybill_num)*1.0/sum(waybill_num) as intm_72_rt 
      ,0.5*sum(intm_78_rt*waybill_num)*1.0/sum(waybill_num) as intm_78_rt 
      ,0.5*sum(intm_84_rt*waybill_num)*1.0/sum(waybill_num) as intm_84_rt 
      ,0.5*sum(intm_90_rt*waybill_num)*1.0/sum(waybill_num) as intm_90_rt 
      ,0.5*sum(intm_96_rt*waybill_num)*1.0/sum(waybill_num) as intm_96_rt 
      ,0.5*sum(intm_2D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_2D12_rt 
      ,0.5*sum(intm_2D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_2D18_rt 
      ,0.5*sum(intm_3D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_3D12_rt 
      ,0.5*sum(intm_3D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_3D18_rt 
      ,0.5*sum(intm_4D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_4D12_rt 
      ,0.5*sum(intm_4D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_4D18_rt 
      ,0.5*sum(intm_that_rt*waybill_num)*1.0/sum(waybill_num) as intm_that_rt 
      ,0.5*sum(intm_next_rt*waybill_num)*1.0/sum(waybill_num) as intm_next_rt 
      ,0.5*sum(intm_other_rt*waybill_num)*1.0/sum(waybill_num) as intm_other_rt 
      ,sum(waybill_num) waybill_num
  from tmp_dm_as.ts_supply_way_tm_flowbase_all_tmp
 where waybill_num>=10 
   and product_code='S2'
   and distance_section in ('(2500-3000]','(3000-3500]','(3500-4000]')
 group by product_code,distance_section;

--重货专运的2500-3000区间段的时效数据用2000-2500区间段代替      
insert into table tmp_dm_as.ts_supply_way_tm_flow_distance_pure_tmp
select product_code
      ,'(2500-3000]' distance_section
      ,avg(avg_od_tm) as avg_od_tm
      ,avg(od_tm_mode) as od_tm_mode
      ,avg(od_tm_05) as od_tm_05
      ,avg(od_tm_25) as od_tm_25
      ,avg(od_tm_40) as od_tm_40
      ,avg(od_tm_45) as od_tm_45
      ,avg(od_tm_mid) as od_tm_mid
      ,avg(od_tm_55) as od_tm_55
      ,avg(od_tm_60) as od_tm_60
      ,avg(od_tm_65) as od_tm_65
      ,avg(od_tm_70) as od_tm_70
      ,avg(od_tm_75) as od_tm_75
      ,avg(od_tm_80) as od_tm_80
      ,avg(od_tm_85) as od_tm_85
      ,avg(od_tm_90) as od_tm_90
      ,avg(od_tm_95) as od_tm_95
      ,sum(intm_24_rt*waybill_num)*1.0/sum(waybill_num) as intm_24_rt 
      ,sum(intm_36_rt*waybill_num)*1.0/sum(waybill_num) as intm_36_rt 
      ,sum(intm_42_rt*waybill_num)*1.0/sum(waybill_num) as intm_42_rt 
      ,sum(intm_48_rt*waybill_num)*1.0/sum(waybill_num) as intm_48_rt 
      ,sum(intm_54_rt*waybill_num)*1.0/sum(waybill_num) as intm_54_rt 
      ,sum(intm_60_rt*waybill_num)*1.0/sum(waybill_num) as intm_60_rt 
      ,sum(intm_66_rt*waybill_num)*1.0/sum(waybill_num) as intm_66_rt 
      ,sum(intm_72_rt*waybill_num)*1.0/sum(waybill_num) as intm_72_rt 
      ,sum(intm_78_rt*waybill_num)*1.0/sum(waybill_num) as intm_78_rt 
      ,sum(intm_84_rt*waybill_num)*1.0/sum(waybill_num) as intm_84_rt 
      ,sum(intm_90_rt*waybill_num)*1.0/sum(waybill_num) as intm_90_rt 
      ,sum(intm_96_rt*waybill_num)*1.0/sum(waybill_num) as intm_96_rt 
      ,sum(intm_2D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_2D12_rt 
      ,sum(intm_2D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_2D18_rt 
      ,sum(intm_3D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_3D12_rt 
      ,sum(intm_3D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_3D18_rt 
      ,sum(intm_4D12_rt*waybill_num)*1.0/sum(waybill_num) as intm_4D12_rt 
      ,sum(intm_4D18_rt*waybill_num)*1.0/sum(waybill_num) as intm_4D18_rt 
      ,sum(intm_that_rt*waybill_num)*1.0/sum(waybill_num) as intm_that_rt 
      ,sum(intm_next_rt*waybill_num)*1.0/sum(waybill_num) as intm_next_rt 
      ,sum(intm_other_rt*waybill_num)*1.0/sum(waybill_num) as intm_other_rt 
      ,sum(waybill_num) waybill_num
  from tmp_dm_as.ts_supply_way_tm_flowbase_all_tmp
 where waybill_num>=10 
   and product_code='SE0020'
   and distance_section in ('(2000-2500]')
 group by product_code,distance_section;

--各产品距离均值，补充3500-4000,4000-5000的情况，只有大票零担SE0114和重货专运SE0020，大票零担用3500-4000补充4000-5000，重货专运用3000-3500补充3500-4000和4000-5000
drop table tmp_dm_as.ts_supply_way_tm_flow_distance_mid_tmp;  
create table tmp_dm_as.ts_supply_way_tm_flow_distance_mid_tmp stored as parquet as
select a.price_product_code product_code
      ,a.distance_section
      ,nvl(nvl(nvl(b.avg_od_tm,c.avg_od_tm ), d.avg_od_tm ), e.avg_od_tm) as avg_od_tm
      ,nvl(nvl(nvl(b.od_tm_mode,c.od_tm_mode ), d.od_tm_mode ), e.od_tm_mode) as od_tm_mode
      ,nvl(nvl(nvl(b.od_tm_05,c.od_tm_05),d.od_tm_05),e.od_tm_05) as od_tm_05
      ,nvl(nvl(nvl(b.od_tm_25,c.od_tm_25),d.od_tm_25),e.od_tm_25) as od_tm_25
      ,nvl(nvl(nvl(b.od_tm_40,c.od_tm_40),d.od_tm_40),e.od_tm_40) as od_tm_40
      ,nvl(nvl(nvl(b.od_tm_45,c.od_tm_45),d.od_tm_45),e.od_tm_45) as od_tm_45
      ,nvl(nvl(nvl(b.od_tm_mid,c.od_tm_mid),d.od_tm_mid),e.od_tm_mid) as od_tm_mid
      ,nvl(nvl(nvl(b.od_tm_55,c.od_tm_55),d.od_tm_55),e.od_tm_55) as od_tm_55
      ,nvl(nvl(nvl(b.od_tm_60,c.od_tm_60),d.od_tm_60),e.od_tm_60) as od_tm_60
      ,nvl(nvl(nvl(b.od_tm_65,c.od_tm_65),d.od_tm_65),e.od_tm_65) as od_tm_65
      ,nvl(nvl(nvl(b.od_tm_70,c.od_tm_70),d.od_tm_70),e.od_tm_70) as od_tm_70
      ,nvl(nvl(nvl(b.od_tm_75,c.od_tm_75),d.od_tm_75),e.od_tm_75) as od_tm_75
      ,nvl(nvl(nvl(b.od_tm_80,c.od_tm_80),d.od_tm_80),e.od_tm_80) as od_tm_80
      ,nvl(nvl(nvl(b.od_tm_85,c.od_tm_85),d.od_tm_85),e.od_tm_85) as od_tm_85
      ,nvl(nvl(nvl(b.od_tm_90,c.od_tm_90),d.od_tm_90),e.od_tm_90) as od_tm_90
      ,nvl(nvl(nvl(b.od_tm_95,c.od_tm_95),d.od_tm_95),e.od_tm_95) as od_tm_95
      ,nvl(nvl(nvl(b.intm_24_rt,c.intm_24_rt),d.intm_24_rt),e.intm_24_rt) as intm_24_rt 
      ,nvl(nvl(nvl(b.intm_36_rt,c.intm_36_rt),d.intm_36_rt),e.intm_36_rt) as intm_36_rt 
      ,nvl(nvl(nvl(b.intm_42_rt,c.intm_42_rt),d.intm_42_rt),e.intm_42_rt) as intm_42_rt 
      ,nvl(nvl(nvl(b.intm_48_rt,c.intm_48_rt),d.intm_48_rt),e.intm_48_rt) as intm_48_rt 
      ,nvl(nvl(nvl(b.intm_54_rt,c.intm_54_rt),d.intm_54_rt),e.intm_54_rt) as intm_54_rt 
      ,nvl(nvl(nvl(b.intm_60_rt,c.intm_60_rt),d.intm_60_rt),e.intm_60_rt) as intm_60_rt 
      ,nvl(nvl(nvl(b.intm_66_rt,c.intm_66_rt),d.intm_66_rt),e.intm_66_rt) as intm_66_rt 
      ,nvl(nvl(nvl(b.intm_72_rt,c.intm_72_rt),d.intm_72_rt),e.intm_72_rt) as intm_72_rt 
      ,nvl(nvl(nvl(b.intm_78_rt,c.intm_78_rt),d.intm_78_rt),e.intm_78_rt) as intm_78_rt 
      ,nvl(nvl(nvl(b.intm_84_rt,c.intm_84_rt),d.intm_84_rt),e.intm_84_rt) as intm_84_rt 
      ,nvl(nvl(nvl(b.intm_90_rt,c.intm_90_rt),d.intm_90_rt),e.intm_90_rt) as intm_90_rt 
      ,nvl(nvl(nvl(b.intm_96_rt,c.intm_96_rt),d.intm_96_rt),e.intm_96_rt) as intm_96_rt 
      ,nvl(nvl(nvl(b.intm_2D12_rt,c.intm_2D12_rt),d.intm_2D12_rt),e.intm_2D12_rt) as intm_2D12_rt 
      ,nvl(nvl(nvl(b.intm_2D18_rt,c.intm_2D18_rt),d.intm_2D18_rt),e.intm_2D18_rt) as intm_2D18_rt 
      ,nvl(nvl(nvl(b.intm_3D12_rt,c.intm_3D12_rt),d.intm_3D12_rt),e.intm_3D12_rt) as intm_3D12_rt 
      ,nvl(nvl(nvl(b.intm_3D18_rt,c.intm_3D18_rt),d.intm_3D18_rt),e.intm_3D18_rt) as intm_3D18_rt 
      ,nvl(nvl(nvl(b.intm_4D12_rt,c.intm_4D12_rt),d.intm_4D12_rt),e.intm_4D12_rt) as intm_4D12_rt 
      ,nvl(nvl(nvl(b.intm_4D18_rt,c.intm_4D18_rt),d.intm_4D18_rt),e.intm_4D18_rt) as intm_4D18_rt 
      ,nvl(nvl(nvl(b.intm_that_rt,c.intm_that_rt),d.intm_that_rt),e.intm_that_rt) as intm_that_rt 
      ,nvl(nvl(nvl(b.intm_next_rt,c.intm_next_rt),d.intm_next_rt),e.intm_next_rt) as intm_next_rt 
      ,nvl(nvl(nvl(b.intm_other_rt,c.intm_other_rt),d.intm_other_rt),e.intm_other_rt) as intm_other_rt
      ,nvl(nvl(nvl(b.waybill_num,c.waybill_num ), d.waybill_num ), e.waybill_num) as waybill_num
      ,case when b.avg_od_tm is null then 1 else 0 end add_mark_distance
  from 
       (select price_product_code
              ,distance_section
              ,distance_section join_distance_section
          from dm_as.ts_supply_distance_section_info --产品距离区间维表
       ) a
       
       left outer join 

       tmp_dm_as.ts_supply_way_tm_flow_distance_pure_tmp b on a.price_product_code=b.product_code and a.distance_section=b.distance_section
       
       left outer join 

       (select product_code
              ,'(4000-5000]' distance_section
              ,avg_od_tm*1.141 avg_od_tm
              ,od_tm_mode*1.145 od_tm_mode      --众数比较波动，选择中位数的系数
              ,od_tm_05*1.102 as od_tm_05
              ,od_tm_25*1.135 as od_tm_25
              ,od_tm_40*1.144 as od_tm_40
              ,od_tm_45*1.145 as od_tm_45
              ,od_tm_mid*1.145 as od_tm_mid
              ,od_tm_55*1.144 as od_tm_55
              ,od_tm_60*1.143 as od_tm_60
              ,od_tm_65*1.143 as od_tm_65
              ,od_tm_70*1.141 as od_tm_70
              ,od_tm_75*1.138 as od_tm_75
              ,od_tm_80*1.135 as od_tm_80
              ,od_tm_85*1.134 as od_tm_85
              ,od_tm_90*1.138 as od_tm_90
              ,od_tm_95*1.153 as od_tm_95
              ,intm_24_rt*0.61 as intm_24_rt
              ,intm_36_rt*0.72 as intm_36_rt
              ,intm_42_rt*0.77 as intm_42_rt
              ,intm_48_rt*0.90 as intm_48_rt
              ,intm_54_rt*0.96 as intm_54_rt
              ,intm_60_rt*0.96 as intm_60_rt
              ,intm_66_rt*0.65 as intm_66_rt
              ,intm_72_rt*0.28 as intm_72_rt
              ,intm_78_rt*0.20 as intm_78_rt
              ,intm_84_rt*0.20 as intm_84_rt
              ,intm_90_rt*0.41 as intm_90_rt
              ,intm_96_rt*0.56 as intm_96_rt
              ,intm_2D12_rt*0.73 as intm_2D12_rt
              ,intm_2D18_rt*0.88 as intm_2D18_rt
              ,intm_3D12_rt*0.76 as intm_3D12_rt
              ,intm_3D18_rt*0.41 as intm_3D18_rt
              ,intm_4D12_rt*0.46 as intm_4D12_rt
              ,intm_4D18_rt*0.55 as intm_4D18_rt
              ,intm_that_rt*0.68 as intm_that_rt
              ,intm_next_rt*0.91 as intm_next_rt
              ,intm_other_rt*0.40 as intm_other_rt
              ,waybill_num
          from tmp_dm_as.ts_supply_way_tm_flow_distance_pure_tmp
         where distance_section='(3500-4000]'  --大票零担需要
       ) c on a.price_product_code=c.product_code and a.join_distance_section=c.distance_section
       
       left outer join 

       (select product_code
              ,'(3500-4000]' distance_section
              ,avg_od_tm*1.128 avg_od_tm
              ,od_tm_mode*1.121 od_tm_mode    --众数比较波动，选择中位数的系数
              ,od_tm_05*1.112 as od_tm_05
              ,od_tm_25*1.133 as od_tm_25
              ,od_tm_40*1.126 as od_tm_40
              ,od_tm_45*1.122 as od_tm_45
              ,od_tm_mid*1.121 as od_tm_mid
              ,od_tm_55*1.120 as od_tm_55
              ,od_tm_60*1.119 as od_tm_60
              ,od_tm_65*1.118 as od_tm_65
              ,od_tm_70*1.118 as od_tm_70
              ,od_tm_75*1.119 as od_tm_75
              ,od_tm_80*1.118 as od_tm_80
              ,od_tm_85*1.119 as od_tm_85
              ,od_tm_90*1.120 as od_tm_90
              ,od_tm_95*1.133 as od_tm_95
              ,intm_24_rt*1.56 as intm_24_rt
              ,intm_36_rt*1.52 as intm_36_rt
              ,intm_42_rt*1.54 as intm_42_rt
              ,intm_48_rt*1.71 as intm_48_rt
              ,intm_54_rt*1.64 as intm_54_rt
              ,intm_60_rt*0.70 as intm_60_rt
              ,intm_66_rt*0.13 as intm_66_rt
              ,intm_72_rt*0.18 as intm_72_rt
              ,intm_78_rt*0.24 as intm_78_rt
              ,intm_84_rt*0.26 as intm_84_rt
              ,intm_90_rt*0.53 as intm_90_rt
              ,intm_96_rt*0.76 as intm_96_rt
              ,intm_2D12_rt*1.46 as intm_2D12_rt
              ,intm_2D18_rt*1.63 as intm_2D18_rt
              ,intm_3D12_rt*0.15 as intm_3D12_rt
              ,intm_3D18_rt*0.19 as intm_3D18_rt
              ,intm_4D12_rt*0.45 as intm_4D12_rt
              ,intm_4D18_rt*0.54 as intm_4D18_rt
              ,intm_that_rt*1.38 as intm_that_rt
              ,intm_next_rt*1.61 as intm_next_rt
              ,intm_other_rt*0.20 as intm_other_rt
              ,waybill_num 
          from tmp_dm_as.ts_supply_way_tm_flow_distance_pure_tmp
         where distance_section='(3000-3500]'   --重货专运需要
       ) d on a.price_product_code=d.product_code and a.join_distance_section=d.distance_section
       
       left outer join 

       (select product_code
              ,'(4000-5000]' distance_section
              ,avg_od_tm*1.288 avg_od_tm
              ,od_tm_mode*1.283 od_tm_mode      --众数比较波动，选择中位数的系数
              ,od_tm_05*1.225 as od_tm_05
              ,od_tm_25*1.285 as od_tm_25
              ,od_tm_40*1.288 as od_tm_40
              ,od_tm_45*1.285 as od_tm_45
              ,od_tm_mid*1.283 as od_tm_mid
              ,od_tm_55*1.281 as od_tm_55
              ,od_tm_60*1.279 as od_tm_60
              ,od_tm_65*1.278 as od_tm_65
              ,od_tm_70*1.275 as od_tm_70
              ,od_tm_75*1.273 as od_tm_75
              ,od_tm_80*1.270 as od_tm_80
              ,od_tm_85*1.270 as od_tm_85
              ,od_tm_90*1.275 as od_tm_90
              ,od_tm_95*1.306 as od_tm_95
              ,intm_24_rt*0.95 as intm_24_rt    --按照特惠的标准，72小时无法送达
              ,intm_36_rt*1.09 as intm_36_rt
              ,intm_42_rt*1.18 as intm_42_rt
              ,intm_48_rt*1.54 as intm_48_rt
              ,intm_54_rt*1.57 as intm_54_rt
              ,intm_60_rt*0.67 as intm_60_rt
              ,intm_66_rt*0.09 as intm_66_rt
              ,intm_72_rt*0.05 as intm_72_rt
              ,intm_78_rt*0.05 as intm_78_rt
              ,intm_84_rt*0.05 as intm_84_rt
              ,intm_90_rt*0.22 as intm_90_rt
              ,intm_96_rt*0.427 as intm_96_rt
              ,intm_2D12_rt*1.07 as intm_2D12_rt
              ,intm_2D18_rt*1.44 as intm_2D18_rt
              ,intm_3D12_rt*0.11 as intm_3D12_rt
              ,intm_3D18_rt*0.08 as intm_3D18_rt
              ,intm_4D12_rt*0.21 as intm_4D12_rt
              ,intm_4D18_rt*0.30 as intm_4D18_rt
              ,intm_that_rt*0.94 as intm_that_rt
              ,intm_next_rt*1.47 as intm_next_rt
              ,intm_other_rt*0.08 as intm_other_rt
              ,waybill_num
          from tmp_dm_as.ts_supply_way_tm_flow_distance_pure_tmp
         where distance_section='(3000-3500]'  --重货专运需要
       ) e on a.price_product_code=e.product_code and a.join_distance_section=e.distance_section;
      

--各产品+距离均值，补充>6000，5000-6000的情况，用5000-6000或4000-5000补充>6000，用4000-5000补充5000-6000
--简单用系数拟合了，鉴于网点间距离不会特别远，所以没有特别严格处理
drop table tmp_dm_as.ts_supply_way_tm_flow_distance_tmp;  
create table tmp_dm_as.ts_supply_way_tm_flow_distance_tmp stored as parquet as
select a.price_product_code product_code
      ,a.distance_section
      ,nvl(nvl(nvl(b.avg_od_tm,c.avg_od_tm ), d.avg_od_tm ), e.avg_od_tm) as avg_od_tm
      ,nvl(nvl(nvl(b.od_tm_mode,c.od_tm_mode ), d.od_tm_mode ), e.od_tm_mode) as od_tm_mode
      ,nvl(nvl(nvl(b.od_tm_05,c.od_tm_05),d.od_tm_05),e.od_tm_05) as od_tm_05
      ,nvl(nvl(nvl(b.od_tm_25,c.od_tm_25),d.od_tm_25),e.od_tm_25) as od_tm_25
      ,nvl(nvl(nvl(b.od_tm_40,c.od_tm_40),d.od_tm_40),e.od_tm_40) as od_tm_40
      ,nvl(nvl(nvl(b.od_tm_45,c.od_tm_45),d.od_tm_45),e.od_tm_45) as od_tm_45
      ,nvl(nvl(nvl(b.od_tm_mid,c.od_tm_mid),d.od_tm_mid),e.od_tm_mid) as od_tm_mid
      ,nvl(nvl(nvl(b.od_tm_55,c.od_tm_55),d.od_tm_55),e.od_tm_55) as od_tm_55
      ,nvl(nvl(nvl(b.od_tm_60,c.od_tm_60),d.od_tm_60),e.od_tm_60) as od_tm_60
      ,nvl(nvl(nvl(b.od_tm_65,c.od_tm_65),d.od_tm_65),e.od_tm_65) as od_tm_65
      ,nvl(nvl(nvl(b.od_tm_70,c.od_tm_70),d.od_tm_70),e.od_tm_70) as od_tm_70
      ,nvl(nvl(nvl(b.od_tm_75,c.od_tm_75),d.od_tm_75),e.od_tm_75) as od_tm_75
      ,nvl(nvl(nvl(b.od_tm_80,c.od_tm_80),d.od_tm_80),e.od_tm_80) as od_tm_80
      ,nvl(nvl(nvl(b.od_tm_85,c.od_tm_85),d.od_tm_85),e.od_tm_85) as od_tm_85
      ,nvl(nvl(nvl(b.od_tm_90,c.od_tm_90),d.od_tm_90),e.od_tm_90) as od_tm_90
      ,nvl(nvl(nvl(b.od_tm_95,c.od_tm_95),d.od_tm_95),e.od_tm_95) as od_tm_95
      ,nvl(nvl(nvl(b.intm_24_rt,c.intm_24_rt),d.intm_24_rt),e.intm_24_rt) as intm_24_rt 
      ,nvl(nvl(nvl(b.intm_36_rt,c.intm_36_rt),d.intm_36_rt),e.intm_36_rt) as intm_36_rt 
      ,nvl(nvl(nvl(b.intm_42_rt,c.intm_42_rt),d.intm_42_rt),e.intm_42_rt) as intm_42_rt 
      ,nvl(nvl(nvl(b.intm_48_rt,c.intm_48_rt),d.intm_48_rt),e.intm_48_rt) as intm_48_rt 
      ,nvl(nvl(nvl(b.intm_54_rt,c.intm_54_rt),d.intm_54_rt),e.intm_54_rt) as intm_54_rt 
      ,nvl(nvl(nvl(b.intm_60_rt,c.intm_60_rt),d.intm_60_rt),e.intm_60_rt) as intm_60_rt 
      ,nvl(nvl(nvl(b.intm_66_rt,c.intm_66_rt),d.intm_66_rt),e.intm_66_rt) as intm_66_rt 
      ,nvl(nvl(nvl(b.intm_72_rt,c.intm_72_rt),d.intm_72_rt),e.intm_72_rt) as intm_72_rt 
      ,nvl(nvl(nvl(b.intm_78_rt,c.intm_78_rt),d.intm_78_rt),e.intm_78_rt) as intm_78_rt 
      ,nvl(nvl(nvl(b.intm_84_rt,c.intm_84_rt),d.intm_84_rt),e.intm_84_rt) as intm_84_rt 
      ,nvl(nvl(nvl(b.intm_90_rt,c.intm_90_rt),d.intm_90_rt),e.intm_90_rt) as intm_90_rt 
      ,nvl(nvl(nvl(b.intm_96_rt,c.intm_96_rt),d.intm_96_rt),e.intm_96_rt) as intm_96_rt 
      ,nvl(nvl(nvl(b.intm_2D12_rt,c.intm_2D12_rt),d.intm_2D12_rt),e.intm_2D12_rt) as intm_2D12_rt 
      ,nvl(nvl(nvl(b.intm_2D18_rt,c.intm_2D18_rt),d.intm_2D18_rt),e.intm_2D18_rt) as intm_2D18_rt 
      ,nvl(nvl(nvl(b.intm_3D12_rt,c.intm_3D12_rt),d.intm_3D12_rt),e.intm_3D12_rt) as intm_3D12_rt 
      ,nvl(nvl(nvl(b.intm_3D18_rt,c.intm_3D18_rt),d.intm_3D18_rt),e.intm_3D18_rt) as intm_3D18_rt 
      ,nvl(nvl(nvl(b.intm_4D12_rt,c.intm_4D12_rt),d.intm_4D12_rt),e.intm_4D12_rt) as intm_4D12_rt 
      ,nvl(nvl(nvl(b.intm_4D18_rt,c.intm_4D18_rt),d.intm_4D18_rt),e.intm_4D18_rt) as intm_4D18_rt 
      ,nvl(nvl(nvl(b.intm_that_rt,c.intm_that_rt),d.intm_that_rt),e.intm_that_rt) as intm_that_rt 
      ,nvl(nvl(nvl(b.intm_next_rt,c.intm_next_rt),d.intm_next_rt),e.intm_next_rt) as intm_next_rt 
      ,nvl(nvl(nvl(b.intm_other_rt,c.intm_other_rt),d.intm_other_rt),e.intm_other_rt) as intm_other_rt
      ,nvl(nvl(nvl(b.waybill_num,c.waybill_num ), d.waybill_num ), e.waybill_num) as waybill_num
      ,case when b.avg_od_tm is null then 1 else 0 end add_mark_distance
  from 
       (select price_product_code
              ,distance_section
              ,case when distance_section like '%6000km%' then '>6000km'     --不知道什么原因匹配不上，只能这样操作
                    else distance_section end join_distance_section
          from dm_as.ts_supply_distance_section_info 
       ) a
       
       left outer join 

       tmp_dm_as.ts_supply_way_tm_flow_distance_mid_tmp b on a.price_product_code=b.product_code and a.distance_section=b.distance_section
       
       left outer join 

       (select product_code
              ,'>6000km' distance_section
              ,avg_od_tm*1.133 avg_od_tm
              ,od_tm_mode*1.145 od_tm_mode      --众数比较波动，选择中位数的系数
              ,od_tm_05*1.225 as od_tm_05
              ,od_tm_25*1.157 as od_tm_25
              ,od_tm_40*1.149 as od_tm_40
              ,od_tm_45*1.151 as od_tm_45
              ,od_tm_mid*1.145 as od_tm_mid
              ,od_tm_55*1.136 as od_tm_55
              ,od_tm_60*1.126 as od_tm_60
              ,od_tm_65*1.122 as od_tm_65
              ,od_tm_70*1.124 as od_tm_70
              ,od_tm_75*1.137 as od_tm_75
              ,od_tm_80*1.156 as od_tm_80
              ,od_tm_85*1.149 as od_tm_85
              ,od_tm_90*1.141 as od_tm_90
              ,od_tm_95*1.089 as od_tm_95
              ,0.0 as intm_24_rt    --按照特惠的标准，72小时无法送达
              ,0.0 as intm_36_rt
              ,0.0 as intm_42_rt
              ,0.0 as intm_48_rt
              ,0.0 as intm_54_rt
              ,0.0 as intm_60_rt
              ,0.0 as intm_66_rt
              ,0.0 as intm_72_rt
              ,0.0 as intm_78_rt
              ,0.0 as intm_84_rt
              ,0.0 as intm_90_rt
              ,0.0 as intm_96_rt
              ,0.0 as intm_2D12_rt
              ,0.0 as intm_2D18_rt
              ,0.0 as intm_3D12_rt
              ,0.0 as intm_3D18_rt
              ,0.0 as intm_4D12_rt
              ,0.0 as intm_4D18_rt
              ,0.0 as intm_that_rt
              ,0.0 as intm_next_rt
              ,0.0 as intm_other_rt
              ,waybill_num
          from tmp_dm_as.ts_supply_way_tm_flow_distance_mid_tmp
         where distance_section='(5000-6000]'
       ) c on a.price_product_code=c.product_code and a.join_distance_section=c.distance_section
       
       left outer join 

       (select product_code
              ,'(5000-6000]' distance_section
              ,avg_od_tm*1.158 avg_od_tm
              ,od_tm_mode*1.160 od_tm_mode
              ,od_tm_05*1.201 as od_tm_05
              ,od_tm_25*1.161 as od_tm_25
              ,od_tm_40*1.155 as od_tm_40
              ,od_tm_45*1.158 as od_tm_45
              ,od_tm_mid*1.160 as od_tm_mid
              ,od_tm_55*1.163 as od_tm_55
              ,od_tm_60*1.165 as od_tm_60
              ,od_tm_65*1.164 as od_tm_65
              ,od_tm_70*1.162 as od_tm_70
              ,od_tm_75*1.159 as od_tm_75
              ,od_tm_80*1.157 as od_tm_80
              ,od_tm_85*1.148 as od_tm_85
              ,od_tm_90*1.142 as od_tm_90
              ,od_tm_95*1.125 as od_tm_95
              ,intm_24_rt*1.38 as intm_24_rt    --按照特惠的标准，72小时无法送达
              ,intm_36_rt*0.75 as intm_36_rt
              ,intm_42_rt*0.28 as intm_42_rt
              ,intm_48_rt*0.14 as intm_48_rt
              ,intm_54_rt*0.14 as intm_54_rt
              ,intm_60_rt*0.14 as intm_60_rt
              ,intm_66_rt*0.21 as intm_66_rt
              ,intm_72_rt*0.31 as intm_72_rt
              ,intm_78_rt*0.34 as intm_78_rt
              ,intm_84_rt*0.31 as intm_84_rt
              ,intm_90_rt*0.08 as intm_90_rt
              ,intm_96_rt*0.06 as intm_96_rt
              ,intm_2D12_rt*0.40 as intm_2D12_rt
              ,intm_2D18_rt*0.21 as intm_2D18_rt
              ,intm_3D12_rt*0.23 as intm_3D12_rt
              ,intm_3D18_rt*0.22 as intm_3D18_rt
              ,intm_4D12_rt*0.08 as intm_4D12_rt
              ,intm_4D18_rt*0.08 as intm_4D18_rt
              ,intm_that_rt*1.73 as intm_that_rt
              ,intm_next_rt*0.21 as intm_next_rt
              ,intm_other_rt*0.22 as intm_other_rt
              ,waybill_num
          from tmp_dm_as.ts_supply_way_tm_flow_distance_mid_tmp
         where distance_section='(4000-5000]'
       ) d on a.price_product_code=d.product_code and a.join_distance_section=d.distance_section 
       
       left outer join 

       (select product_code
              ,'>6000km' distance_section
              ,avg_od_tm*1.338 avg_od_tm
              ,od_tm_mode*1.359 od_tm_mode      --众数比较波动，选择中位数的系数
              ,od_tm_05*1.471 as od_tm_05
              ,od_tm_25*1.343 as od_tm_25
              ,od_tm_40*1.327 as od_tm_40
              ,od_tm_45*1.333 as od_tm_45
              ,od_tm_mid*1.328 as od_tm_mid
              ,od_tm_55*1.320 as od_tm_55
              ,od_tm_60*1.311 as od_tm_60
              ,od_tm_65*1.305 as od_tm_65
              ,od_tm_70*1.306 as od_tm_70
              ,od_tm_75*1.318 as od_tm_75
              ,od_tm_80*1.337 as od_tm_80
              ,od_tm_85*1.319 as od_tm_85
              ,od_tm_90*1.303 as od_tm_90
              ,od_tm_95*1.226 as od_tm_95
              ,0.0 as intm_24_rt    --按照特惠的标准，72小时无法送达
              ,0.0 as intm_36_rt
              ,0.0 as intm_42_rt
              ,0.0 as intm_48_rt
              ,0.0 as intm_54_rt
              ,0.0 as intm_60_rt
              ,0.0 as intm_66_rt
              ,0.0 as intm_72_rt
              ,0.0 as intm_78_rt
              ,0.0 as intm_84_rt
              ,0.0 as intm_90_rt
              ,0.0 as intm_96_rt
              ,0.0 as intm_2D12_rt
              ,0.0 as intm_2D18_rt
              ,0.0 as intm_3D12_rt
              ,0.0 as intm_3D18_rt
              ,0.0 as intm_4D12_rt
              ,0.0 as intm_4D18_rt
              ,0.0 as intm_that_rt
              ,0.0 as intm_next_rt
              ,0.0 as intm_other_rt
              ,waybill_num
          from tmp_dm_as.ts_supply_way_tm_flow_distance_mid_tmp
         where distance_section='(4000-5000]'
       ) e on a.price_product_code=e.product_code and a.join_distance_section=e.distance_section;
      
---------------------------------------------------------------------------
--填补所有流向数据，优先用产品+省份+距离区间维度的均值，其次用产品+距离区间均值替代
drop table tmp_dm_as.ts_supply_way_tm_flow_tmp;
create table tmp_dm_as.ts_supply_way_tm_flow_tmp stored as parquet as
select a.product_code,a.product_name,a.flow_product_code,a.src_code,a.src_city,a.src_province,a.src_dist,a.src_area,a.dest_code,a.dest_city,a.dest_province,a.dest_dist,a.dest_area,a.area_type,a.distance,a.distance_section
      ,case when a.avg_od_tm is null then 1 else 0 end as add_mark
      ,nvl(nvl(a.avg_od_tm  ,b.avg_od_tm  ), c.avg_od_tm  )  as avg_od_tm
      ,nvl(nvl(a.od_tm_mode ,b.od_tm_mode ), c.od_tm_mode )  as od_tm_mode
      ,nvl(nvl(a.od_tm_05,b.od_tm_05),c.od_tm_05) as od_tm_05
      ,nvl(nvl(a.od_tm_25,b.od_tm_25),c.od_tm_25) as od_tm_25
      ,nvl(nvl(a.od_tm_40,b.od_tm_40),c.od_tm_40) as od_tm_40
      ,nvl(nvl(a.od_tm_45,b.od_tm_45),c.od_tm_45) as od_tm_45
      ,nvl(nvl(a.od_tm_mid,b.od_tm_mid),c.od_tm_mid) as od_tm_mid
      ,nvl(nvl(a.od_tm_55,b.od_tm_55),c.od_tm_55) as od_tm_55
      ,nvl(nvl(a.od_tm_60,b.od_tm_60),c.od_tm_60) as od_tm_60
      ,nvl(nvl(a.od_tm_65,b.od_tm_65),c.od_tm_65) as od_tm_65
      ,nvl(nvl(a.od_tm_70,b.od_tm_70),c.od_tm_70) as od_tm_70
      ,nvl(nvl(a.od_tm_75,b.od_tm_75),c.od_tm_75) as od_tm_75
      ,nvl(nvl(a.od_tm_80,b.od_tm_80),c.od_tm_80) as od_tm_80
      ,nvl(nvl(a.od_tm_85,b.od_tm_85),c.od_tm_85) as od_tm_85
      ,nvl(nvl(a.od_tm_90,b.od_tm_90),c.od_tm_90) as od_tm_90
      ,nvl(nvl(a.od_tm_95,b.od_tm_95),c.od_tm_95) as od_tm_95
      ,nvl(nvl(a.intm_24_rt,b.intm_24_rt),c.intm_24_rt) as intm_24_rt 
      ,nvl(nvl(a.intm_36_rt,b.intm_36_rt),c.intm_36_rt) as intm_36_rt 
      ,nvl(nvl(a.intm_42_rt,b.intm_42_rt),c.intm_42_rt) as intm_42_rt 
      ,nvl(nvl(a.intm_48_rt,b.intm_48_rt),c.intm_48_rt) as intm_48_rt 
      ,nvl(nvl(a.intm_54_rt,b.intm_54_rt),c.intm_54_rt) as intm_54_rt 
      ,nvl(nvl(a.intm_60_rt,b.intm_60_rt),c.intm_60_rt) as intm_60_rt 
      ,nvl(nvl(a.intm_66_rt,b.intm_66_rt),c.intm_66_rt) as intm_66_rt 
      ,nvl(nvl(a.intm_72_rt,b.intm_72_rt),c.intm_72_rt) as intm_72_rt 
      ,nvl(nvl(a.intm_78_rt,b.intm_78_rt),c.intm_78_rt) as intm_78_rt 
      ,nvl(nvl(a.intm_84_rt,b.intm_84_rt),c.intm_84_rt) as intm_84_rt 
      ,nvl(nvl(a.intm_90_rt,b.intm_90_rt),c.intm_90_rt) as intm_90_rt 
      ,nvl(nvl(a.intm_96_rt,b.intm_96_rt),c.intm_96_rt) as intm_96_rt 
      ,nvl(nvl(a.intm_2D12_rt,b.intm_2D12_rt),c.intm_2D12_rt) as intm_2D12_rt 
      ,nvl(nvl(a.intm_2D18_rt,b.intm_2D18_rt),c.intm_2D18_rt) as intm_2D18_rt 
      ,nvl(nvl(a.intm_3D12_rt,b.intm_3D12_rt),c.intm_3D12_rt) as intm_3D12_rt 
      ,nvl(nvl(a.intm_3D18_rt,b.intm_3D18_rt),c.intm_3D18_rt) as intm_3D18_rt 
      ,nvl(nvl(a.intm_4D12_rt,b.intm_4D12_rt),c.intm_4D12_rt) as intm_4D12_rt 
      ,nvl(nvl(a.intm_4D18_rt,b.intm_4D18_rt),c.intm_4D18_rt) as intm_4D18_rt 
      ,nvl(nvl(a.intm_that_rt,b.intm_that_rt),c.intm_that_rt) as intm_that_rt 
      ,nvl(nvl(a.intm_next_rt,b.intm_next_rt),c.intm_next_rt) as intm_next_rt 
      ,nvl(nvl(a.intm_other_rt,b.intm_other_rt),c.intm_other_rt) as intm_other_rt
      ,nvl(nvl(a.waybill_num,b.waybill_num), c.waybill_num)  as waybill_num
  from 
       tmp_dm_as.ts_supply_way_tm_flowbase_all_tmp a
       
       left join 

       tmp_dm_as.ts_supply_way_tm_flow_province_tmp b on a.product_code=b.product_code and a.src_province=b.src_province and a.dest_province=b.dest_province and a.distance_section=b.distance_section
       
       left join 

       tmp_dm_as.ts_supply_way_tm_flow_distance_tmp c on a.product_code=c.product_code and a.distance_section=c.distance_section;
            
        
--汇总所有时效数据
create table if not exists dm_as.ts_supply_way_tm_avg_percentile_m
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
    od_tm_25           double   comment '25百分位时效',
    od_tm_75           double   comment '75百分位时效',	 
    intm_24_rt         double   comment '24小时达成率',
    intm_48_rt         double   comment '48小时达成率',
    intm_72_rt	       double   comment '72小时达成率',
    waybill_num      bigint comment '运单数量',
    wh_avg_od_tm     double comment '顺丰仓发出-平均时效hour',
    wh_od_tm_05      double comment '顺丰仓发出-最快时效hour',
    wh_od_tm_mid     double comment '顺丰仓发出-中位数时效hour',
    wh_od_tm_95      double comment '顺丰仓发出-最慢时效hour',
    wh_od_tm_mode    double comment '顺丰仓发出-众数时效hour',
    wh_od_tm_25      double comment '顺丰仓发出-25百分位时效',
    wh_od_tm_75      double comment '顺丰仓发出-75百分位时效',
    wh_intm_24_rt    double comment '顺丰仓发出-24小时达成率',
    wh_intm_48_rt    double comment '顺丰仓发出-48小时达成率',
    wh_intm_72_rt    double comment '顺丰仓发出-72小时达成率',
    wh_waybill_num   double comment '顺丰仓发出-运单数量',
    flow_product_code       string   comment '流向产品代码'
)partitioned by (inc_month string) stored as parquet;

--剔除仓发货10条以下记录的
alter table dm_as.ts_supply_way_tm_avg_percentile_m drop partition (inc_month = '$[month(yyyyMM,-1)]');
insert overwrite table dm_as.ts_supply_way_tm_avg_percentile_m partition (inc_month='$[month(yyyyMM,-1)]')
select a.src_code,a.src_city,a.src_province,a.src_dist,a.src_area,a.dest_code,a.dest_city,a.dest_province,a.dest_dist,a.dest_area,a.distance,a.distance_section
      ,a.product_code
      ,a.product_name
      ,a.add_mark
      ,a.avg_od_tm
      ,a.od_tm_05
      ,a.od_tm_mid
      ,a.od_tm_95
      ,a.od_tm_mode
      ,a.od_tm_25
      ,a.od_tm_75
      ,a.intm_24_rt
      ,a.intm_48_rt
      ,a.intm_72_rt
      ,a.waybill_num
      ,b.wh_avg_od_tm
      ,od_tm_list[0] as wh_od_tm_05
      ,od_tm_list[2] as wh_od_tm_mid
      ,od_tm_list[5] as wh_od_tm_95
      ,c.wh_od_tm_mode
      ,od_tm_list[1] as wh_od_tm_25
      ,od_tm_list[3] as wh_od_tm_75
      ,b.wh_intm_24_rt
      ,b.wh_intm_48_rt
      ,b.wh_intm_72_rt
      ,b.wh_waybill_num
      ,a.flow_product_code
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
      ,od_tm_list[4] as wh_od_tm_85
  from 
       tmp_dm_as.ts_supply_way_tm_flow_tmp a  --填补完整后的流向时效数据
       
       left outer join 
       
       (select *
          from dm_as.dm_way_whl_tm_percentile_info_mm a  --仓库发货的流向时效数据
         where inc_month='$[month(yyyyMM,-1)]'
           and wh_waybill_num>=10
       ) b on a.flow_product_code=b.flow_product_code and a.src_code=b.src_dist_code and a.dest_code=b.dest_dist_code
       
       left outer join 
       
       tmp_dm_as.ts_supply_way_whl_tm_mode_tmp c --仓库发货的流向众数数据
       on a.flow_product_code=c.flow_product_code and a.src_code=c.src_dist_code and a.dest_code=c.dest_dist_code;