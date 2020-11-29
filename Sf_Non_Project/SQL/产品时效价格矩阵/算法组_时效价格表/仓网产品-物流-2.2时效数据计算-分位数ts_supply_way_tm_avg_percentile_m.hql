--参数配置
--默认队列任务不能超过500个reduce，则限定以下
set hive.exec.reducers.max=499;

-------------------**********************----------------
--基础时效计算
--时效计算各个分位数计算
--这里使用的编码，是流向表的编码，用flow_product_code标识
--先算出需要的时效分位数
alter table dm_as.dm_supply_way_tm_percentile_info_mm drop partition (inc_month='$[month(yyyyMM,-1)]');
insert overwrite table dm_as.dm_supply_way_tm_percentile_info_mm partition (inc_month='$[month(yyyyMM,-1)]')
select case when product_code='S1' then 'SE0001'
            when product_code='S2' then 'SE0004'
            else product_code end as flow_product_code
      ,src_dist_code,dest_dist_code
      ,percentile_approx(od_tm, array(0.05,0.25,0.4,0.45,0.5,0.55,0.6,0.65,0.7,0.75,0.8,0.85,0.9,0.95),3000) as od_tm_list
      ,src_city_code,dest_city_code
  from dm_as.ts_supply_waybill_tm_info_m a
 where inc_month = '$[month(yyyyMM,-1)]'
   and error_type is null
   and od_tm > 0
 group by case when product_code='S1' then 'SE0001'
               when product_code='S2' then 'SE0004'
               else product_code end
      ,src_dist_code,dest_dist_code,src_city_code,dest_city_code;


--计算时效达成率，并合并时效分位数
drop table tmp_dm_as.ts_supply_way_tm_avg_percentile_tmp;
create table tmp_dm_as.ts_supply_way_tm_avg_percentile_tmp stored as parquet as
select a.flow_product_code,a.src_dist_code,a.src_city_code,a.dest_dist_code,a.dest_city_code
      ,od_tm_05,od_tm_25,od_tm_40,od_tm_45,od_tm_mid,od_tm_55,od_tm_60,od_tm_65,od_tm_70,od_tm_75,od_tm_80,od_tm_85,od_tm_90,od_tm_95
      ,waybill_num,avg_od_tm,intm_24_num,intm_36_num,intm_42_num,intm_48_num,intm_54_num,intm_60_num,intm_66_num,intm_72_num,intm_78_num,intm_84_num,intm_90_num,intm_96_num
      ,intm_2D12_num,intm_2D18_num,intm_3D12_num,intm_3D18_num,intm_4D12_num,intm_4D18_num,intm_that_num,intm_next_num,intm_other_num
  from 
       (select flow_product_code,src_dist_code,src_city_code,dest_dist_code,dest_city_code
              ,round(od_tm_list[0],3) as od_tm_05
              ,round(od_tm_list[1],3) as od_tm_25
              ,round(od_tm_list[2],3) as od_tm_40
              ,round(od_tm_list[3],3) as od_tm_45
              ,round(od_tm_list[4],3) as od_tm_mid
              ,round(od_tm_list[5],3) as od_tm_55
              ,round(od_tm_list[6],3) as od_tm_60
              ,round(od_tm_list[7],3) as od_tm_65
              ,round(od_tm_list[8],3) as od_tm_70
              ,round(od_tm_list[9],3) as od_tm_75
              ,round(od_tm_list[10],3) as od_tm_80
              ,round(od_tm_list[11],3) as od_tm_85
              ,round(od_tm_list[12],3) as od_tm_90
              ,round(od_tm_list[13],3) as od_tm_95
          from dm_as.dm_supply_way_tm_percentile_info_mm
         where inc_month='$[month(yyyyMM,-1)]'
       ) a 

       left outer join 

       (select case when product_code='S1' then 'SE0001'
                    when product_code='S2' then 'SE0004'
                    else product_code end flow_product_code
              ,src_dist_code,src_city_code,dest_dist_code,dest_city_code
              ,count(*) waybill_num
              ,avg(od_tm) as avg_od_tm
              ,sum(case when od_tm<=24 then 1 else 0 end) intm_24_num
              ,sum(case when od_tm<=36 then 1 else 0 end) intm_36_num
              ,sum(case when od_tm<=42 then 1 else 0 end) intm_42_num
              ,sum(case when od_tm<=48 then 1 else 0 end) intm_48_num
              ,sum(case when od_tm<=54 then 1 else 0 end) intm_54_num
              ,sum(case when od_tm<=60 then 1 else 0 end) intm_60_num
              ,sum(case when od_tm<=66 then 1 else 0 end) intm_66_num
              ,sum(case when od_tm<=72 then 1 else 0 end) intm_72_num
              ,sum(case when od_tm<=78 then 1 else 0 end) intm_78_num
              ,sum(case when od_tm<=84 then 1 else 0 end) intm_84_num
              ,sum(case when od_tm<=90 then 1 else 0 end) intm_90_num
              ,sum(case when od_tm<=96 then 1 else 0 end) intm_96_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_add(consigned_tm,1),' ','12:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,2),' ','12:00:00') then 1 else 0 end) as intm_2D12_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_add(consigned_tm,1),' ','18:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,2),' ','18:00:00') then 1 else 0 end) as intm_2D18_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_add(consigned_tm,2),' ','12:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,3),' ','12:00:00') then 1 else 0 end) as intm_3D12_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_add(consigned_tm,2),' ','18:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,3),' ','18:00:00') then 1 else 0 end) as intm_3D18_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_add(consigned_tm,3),' ','12:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,4),' ','12:00:00') then 1 else 0 end) as intm_4D12_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_add(consigned_tm,3),' ','18:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,4),' ','18:00:00') then 1 else 0 end) as intm_4D18_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_format(consigned_tm,'yyyy-MM-dd'),' ','24:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,1),' ','24:00:00') then 1 else 0 end) as intm_that_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_add(consigned_tm,1),' ','24:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,2),' ','24:00:00') then 1 else 0 end) as intm_next_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_add(consigned_tm,2),' ','24:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,3),' ','24:00:00') then 1 else 0 end) as intm_other_num
          from dm_as.ts_supply_waybill_tm_info_m a
         where inc_month='$[month(yyyyMM,-1)]'
           and error_type is null
           and od_tm>0
         group by case when product_code='S1' then 'SE0001'
                       when product_code='S2' then 'SE0004'
                       else product_code end
              ,src_dist_code,src_city_code,dest_dist_code,dest_city_code
       ) b on a.flow_product_code=b.flow_product_code and a.src_dist_code=b.src_dist_code and a.dest_dist_code=b.dest_dist_code;
          


