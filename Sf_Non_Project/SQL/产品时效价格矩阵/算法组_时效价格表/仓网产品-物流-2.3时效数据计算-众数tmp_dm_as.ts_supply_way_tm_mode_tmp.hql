--参数配置
set hive.exec.reducers.max=499;

--众数计算
drop table tmp_dm_as.ts_supply_way_tm_mode_tmp;
create table tmp_dm_as.ts_supply_way_tm_mode_tmp stored as parquet as
    select product_code flow_product_code
           ,src_dist_code,src_city_code,dest_dist_code,dest_city_code
           ,od_tm_hour od_tm_mode
           ,od_tm_num
      from (select product_code,src_dist_code,src_city_code,dest_dist_code,dest_city_code,od_tm_hour,od_tm_num
                   ,row_number() over(partition by product_code,src_dist_code,dest_dist_code order by od_tm_num desc) rnk 
              from (select case when product_code='S1' then 'SE0001'
                                when product_code='S2' then 'SE0004'
                                else product_code
                            end product_code
                           ,src_dist_code
                           ,src_city_code
                           ,dest_dist_code
                           ,dest_city_code
                           ,round(od_tm,0) as od_tm_hour
                           ,count(*) as od_tm_num
                      from dm_as.ts_supply_waybill_tm_info_m a
                      where inc_month='$[month(yyyyMM,-1)]'
                        and error_type is null
                        and od_tm>0
                      group by case when product_code='S1' then 'SE0001'
                                when product_code='S2' then 'SE0004'
                                else product_code
                            end
                        ,src_dist_code
                        ,src_city_code
                        ,dest_dist_code
                        ,dest_city_code
                        ,round(od_tm,0)
                    ) a
           ) a
      where rnk=1;
        
