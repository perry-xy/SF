-------------------**********************----------------
--仓发出的件时效计算-------------------------	
----从仓发出的件时效计算 各流向均值，中位数，百分位数，时效仍然是用的od_tm，但只统计仓库发出的数据
--将时效分位数用list的形式展示
alter table dm_as.dm_way_whl_tm_percentile_info_mm drop partition (inc_month='$[month(yyyyMM,-1)]');
insert overwrite table dm_as.dm_way_whl_tm_percentile_info_mm partition (inc_month='$[month(yyyyMM,-1)]')
    select product_code as flow_product_code
           ,src_dist_code
           ,src_city_code
           ,dest_dist_code
           ,dest_city_code
           ,count(*) as wh_waybill_num
           ,avg(od_tm) as wh_avg_od_tm
           ,percentile_approx(od_tm, array(0.05,0.25,0.5,0.75,0.85,0.95),3000) as od_tm_list
           ,sum(case when od_tm<=24 then 1 else 0 end)*1.0/count(*) wh_intm_24_rt
           ,sum(case when od_tm<=48 then 1 else 0 end)*1.0/count(*) wh_intm_48_rt
           ,sum(case when od_tm<=72 then 1 else 0 end)*1.0/count(*) wh_intm_72_rt
        from dm_as.ts_supply_waybill_whl_tm_m a
        where inc_month='$[month(yyyyMM,-1)]'
          and error_type is null
          and whl_error_type is null
          and od_tm>0
        group by product_code,src_dist_code,src_city_code,dest_dist_code,dest_city_code;

--仓库各流向众数
drop table tmp_dm_as.ts_supply_way_whl_tm_mode_tmp;    
create table tmp_dm_as.ts_supply_way_whl_tm_mode_tmp stored as parquet as
    select product_code flow_product_code
           ,src_dist_code
           ,src_city_code
           ,dest_dist_code
           ,dest_city_code
           ,od_tm_hour as wh_od_tm_mode
      from (select *,row_number() over(partition by product_code,src_dist_code,dest_dist_code order by od_tm_num desc) rn 
              from (select product_code
                           ,src_dist_code
                           ,src_city_code
                           ,dest_dist_code
                           ,dest_city_code
                           ,round(od_tm,0) as od_tm_hour
                           ,count(*) as od_tm_num
                    from dm_as.ts_supply_waybill_whl_tm_m a
                    where inc_month='$[month(yyyyMM,-1)]'
                      and error_type is null
                      and whl_error_type is null
                      and od_tm>0
                    group by product_code,src_dist_code,src_city_code,dest_dist_code,dest_city_code,round(od_tm,0)  
                    ) a 
            ) b 
        where b.rn = 1 ;
        