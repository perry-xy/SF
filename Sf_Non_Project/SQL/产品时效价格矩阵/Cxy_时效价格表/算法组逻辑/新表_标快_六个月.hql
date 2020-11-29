  --标快的价格信息
  set hive.exec.reducers.max=499;
  create table if not exists tmp_dm_as.cxy_new_biaokuai_flow_price stored as parquet as 
  select /*+mapjoin(a)*/ a.product_code
           ,a.product_name
           ,b.source_code src_code
           ,b.source_name src_name
           ,b.dest_code
           ,b.dest_name
           ,b.pay_country
           ,b.price_code
           ,b.base_weight_qty
           ,b.base_price
           ,b.weight_price_qty
           ,b.max_weight_qty
           ,b.light_factor
           ,cast(1 as int) od_normal 
      from (select price_product_code product_code  --标快：price_product_code=S1,flow_product_code=SE0001
                   ,price_product_name product_name
              from dm_as.ts_supply_product_name_info a
              where is_valid=1
           ) a
      join 
                   (select product_name,product_code,source_code,source_name,dest_code,dest_name,pay_country,price_code,base_weight_qty,base_price,weight_price_qty,max_weight_qty,light_factor
                      from dm_as.joanna_price_base_all a
                      where inc_day ='20200831'
                        and flow_type = '1'
                        and base_weight_qty>=0
                        and base_price>=0
                        and weight_price_qty>=0
                        and max_weight_qty>=0
                        and light_factor>=0
                        and product_code='S1'
            ) b on a.product_code=b.product_code
      join (--标识当前在用的流向
            select product_code,source_code,dest_code 
              from (select product_code,source_code,dest_code
                      from dm_pvs.tm_product_flow --新流向表
                      where inc_month ='20200831'
                        and invalid_date is null
                        and source_code is not null 
                        and dest_code is not null
                        and flow_type = '1'
                    union all
                    select product_code,source_code,dest_code
                      from ods_pvs.tm_price_flow   --老流向表
                      where inc_month = '202008'
                        and invalid_date is null
                        and source_code is not null 
                        and dest_code is not null
                        and outside = '0'  
                    ) a
              group by product_code,source_code,dest_code
            ) c on b.product_code=c.product_code and b.source_code=c.source_code and b.dest_code=c.dest_code;



--时效基础数据
--标识转寄、退回、放入合作点中的件
create table if not exists tmp_dm_as.cxy_new_biaokuai_waybill_abnormal stored as parquet as 
    select mainwaybillno
           ,forward_return_bill
           ,handover_bill
           ,barscantm
      from (select mainwaybillno
                   ,case when opcode in('99','870') then 1 else 0 end forward_return_bill      --转寄退回操作的件
                   ,case when opcode in('125','657') then 1 else 0 end handover_bill      --放入丰巢柜,或交接给合作点
                   ,row_number() over(partition by mainwaybillno order by barscantm) rnk
                   ,barscantm
              from dm_as.whl_od_tm_fvp_125
              where inc_day between '20200301' and '20200831'
                and opcode in ('99','870','125','657')
           ) a 
      where rnk=1;

--运单表中取基础数据
create table if not exists tmp_dm_as.cxy_new_biaokuai_waybill_tm_info stored as parquet as
select a.waybill_no
           ,a.source_zone_code 
           ,a.dest_zone_code
           ,a.src_dist_code   --755
           ,a.src_city_code    --ShenZhen
           ,a.src_county
           ,a.dest_dist_code
           ,a.dest_city_code
           ,a.dest_county
           ,a.product_code
           ,a.freight_monthly_acct_code
           ,a.cod_monthly_acct_code
           ,a.inc_day
           ,a.od_tm_org
           ,(unix_timestamp(case when forward_return_bill=0 and handover_bill>=1 and unix_timestamp(b.barscantm) < unix_timestamp(a.signin_tm) then b.barscantm else a.signin_tm end)- unix_timestamp(consigned_tm)) / 3600 as od_tm
           ,a.consigned_tm
           ,a.signin_tm
           ,case when a.error_last_abnormal_tm>=1 then '最后派件时间异常'
                when source_zone_code = dest_zone_code  and od_tm_org>=48 then '同地区超过48小时'
                when b.forward_return_bill>=1 then '转寄和退回的单'
                when substr(consigned_tm,6,5)>='11-05' and substr(consigned_tm,6,5)<='11-30' then '双11的单'
                when substr(barscantm,6,5)>='11-05' and substr(barscantm,6,5)<='11-30' then '双11的单（巴枪）'
                else cast(null as string)
            end error_type
           ,case when b.handover_bill>=1 then 1 else 0 end handover_bill
           ,b.barscantm
      from (select waybill_no
                    ,source_zone_code
                    ,dest_zone_code
                    ,src_dist_code   --755
                    ,src_city_code    --ShenZhen
                    ,src_county     --南山区
                    ,dest_dist_code
                    ,dest_city_code
                    ,dest_county
                    ,product_code
                    ,freight_monthly_acct_code
                    ,cod_monthly_acct_code
                    ,nvl(substr(recv_bar_tm,1,19),consigned_tm) as consigned_tm
                    ,nvl(substr(send_bar_tm,1,19),signin_tm)  as signin_tm
                    ,(unix_timestamp(nvl(substr(send_bar_tm,1,19),signin_tm)) - unix_timestamp(nvl(substr(recv_bar_tm,1,19),consigned_tm))) / 3600 as od_tm_org
                    ,inc_day
                    ,max(case when last_abnormal_tm is null then 0 else 1 end) error_last_abnormal_tm
              from gdl.tt_waybill_info a
              where inc_day between '20200301' and '20200831'
                and src_dist_code is not null       --剔除信息异常的数据
                and dest_dist_code is not null
                and product_code is not null
                and product_code in ('S1','SE0001')
              group by waybill_no,source_zone_code,dest_zone_code,src_dist_code,src_city_code,src_county,dest_dist_code,dest_city_code,dest_county,product_code,freight_monthly_acct_code,cod_monthly_acct_code,nvl(substr(recv_bar_tm,1,19),consigned_tm),nvl(substr(send_bar_tm,1,19),signin_tm),inc_day
            ) a
      left outer join tmp_dm_as.cxy_new_biaokuai_waybill_abnormal b on a.waybill_no=b.mainwaybillno;


--顺丰仓订单时效数据
create table if not exists tmp_dm_as.cxy_new_biaokuai_warehouse_od_tm stored as parquet as 
    select waybill_no
           ,warehouse_code
           ,warehouse_name
           ,carrier_serviece_name
           ,actual_shipment_date
           ,error_type
      from (select waybill_no
                    ,warehouse_code
                    ,warehouse_name
                    ,carrier_serviece_name
                    ,SUBSTRING(shipment_date,0,10) as actual_shipment_date
                    ,row_number() over(partition by waybill_no order by shipment_date) rnk
                    ,case when substr(shipment_date,6,5)>='11-01' and substr(shipment_date,6,5)<='11-30' then '双11的单'
                        else cast(null as string) end error_type
              FROM gdl.mm_shipment_item_info a
              where inc_day between '20200301' and '20200831'
            )  t 
        where rnk = 1;


--关联运单表，作为从仓发货的基础数据
 create table if not exists tmp_dm_as.cxy_new_biaokuai_warehouse_od stored as parquet as
    select a.waybill_no,a.source_zone_code,a.dest_zone_code,a.src_dist_code,a.src_city_code,a.dest_dist_code,a.dest_city_code,a.product_code,a.freight_monthly_acct_code,a.cod_monthly_acct_code,a.inc_day,a.od_tm_org,a.od_tm,a.consigned_tm,a.signin_tm,a.error_type,a.handover_bill,a.barscantm
           ,b.warehouse_code,b.warehouse_name,b.carrier_serviece_name,b.actual_shipment_date,b.error_type whl_error_type
    from (select *
            from tmp_dm_as.cxy_new_biaokuai_waybill_tm_info a
         ) a 
    join tmp_dm_as.cxy_new_biaokuai_warehouse_od_tm b 
        on a.waybill_no = b.waybill_no
    where warehouse_code is not null ;



    --基础时效计算
--时效计算各个分位数计算
--这里使用的编码，是流向表的编码，用flow_product_code标识
--先算出需要的时效分位数
create table if not exists tmp_dm_as.cxy_new_biaokuai_tm_percentile stored as parquet as
select case when product_code='S1' then 'SE0001'
            else product_code end as flow_product_code
      ,src_dist_code,dest_dist_code
      ,percentile_approx(od_tm, 0.05,3000) od_tm_05
      ,percentile_approx(od_tm, 0.25,3000) od_tm_25
      ,percentile_approx(od_tm, 0.50,3000) od_tm_mid
      ,percentile_approx(od_tm, 0.75,3000) od_tm_75 
      ,percentile_approx(od_tm, 0.95,3000) od_tm_95
      ,src_city_code,dest_city_code
  from tmp_dm_as.cxy_new_biaokuai_waybill_tm_info a
 where error_type is null
   and od_tm > 0
 group by case when product_code='S1' then 'SE0001'
               else product_code end
      ,src_dist_code,dest_dist_code,src_city_code,dest_city_code;


--计算时效达成率，并合并时效分位数
create table tmp_dm_as.cxy_new_biaokuai_per_perget stored as parquet as 
select a.flow_product_code,a.src_dist_code,a.src_city_code,a.dest_dist_code,a.dest_city_code
      ,od_tm_05,od_tm_25,od_tm_mid,od_tm_75,od_tm_95
      ,waybill_num,avg_od_tm,intm_24_num,intm_48_num,intm_72_num
      ,intm_that_num,intm_next_num,intm_other_num
  from 
       (select flow_product_code,src_dist_code,src_city_code,dest_dist_code,dest_city_code
              ,od_tm_05
              ,od_tm_25
              ,od_tm_mid
              ,od_tm_75
              ,od_tm_95
          from tmp_dm_as.cxy_new_biaokuai_tm_percentile
       ) a 

       left outer join 

       (select case when product_code='S1' then 'SE0001'
                    else product_code end flow_product_code
              ,src_dist_code,src_city_code,dest_dist_code,dest_city_code
              ,count(*) waybill_num
              ,avg(od_tm) as avg_od_tm
              ,sum(case when od_tm<=24 then 1 else 0 end) intm_24_num
              ,sum(case when od_tm<=48 then 1 else 0 end) intm_48_num
              ,sum(case when od_tm<=72 then 1 else 0 end) intm_72_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_format(consigned_tm,'yyyy-MM-dd'),' ','24:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,1),' ','24:00:00') then 1 else 0 end) as intm_that_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_add(consigned_tm,1),' ','24:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,2),' ','24:00:00') then 1 else 0 end) as intm_next_num
              ,sum(case when substr(consigned_tm,12,8)<='18:00:00' and signin_tm<=concat(date_add(consigned_tm,2),' ','24:00:00') or substr(consigned_tm,12,8)>='18:00:00' and signin_tm<=concat(date_add(consigned_tm,3),' ','24:00:00') then 1 else 0 end) as intm_other_num
          from tmp_dm_as.cxy_new_biaokuai_waybill_tm_info a
         where error_type is null
           and od_tm>0
         group by case when product_code='S1' then 'SE0001'
                       else product_code end
              ,src_dist_code,src_city_code,dest_dist_code,dest_city_code
       ) b on a.flow_product_code=b.flow_product_code and a.src_dist_code=b.src_dist_code and a.dest_dist_code=b.dest_dist_code;


--众数计算
create table tmp_dm_as.cxy_new_biaokuai_tm_mode stored as parquet as
    select product_code flow_product_code
           ,src_dist_code,src_city_code,dest_dist_code,dest_city_code
           ,od_tm_hour od_tm_mode
           ,od_tm_num
      from (select product_code,src_dist_code,src_city_code,dest_dist_code,dest_city_code,od_tm_hour,od_tm_num
                   ,row_number() over(partition by product_code,src_dist_code,dest_dist_code order by od_tm_num desc) rnk 
              from (select case when product_code='S1' then 'SE0001'
                                else product_code
                            end product_code
                           ,src_dist_code
                           ,src_city_code
                           ,dest_dist_code
                           ,dest_city_code
                           ,round(od_tm,0) as od_tm_hour
                           ,count(*) as od_tm_num
                      from tmp_dm_as.cxy_new_biaokuai_waybill_tm_info a
                      where error_type is null
                        and od_tm>0
                      group by case when product_code='S1' then 'SE0001'
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



-------------------**********************----------------
--仓发出的件时效计算-------------------------	
----从仓发出的件时效计算 各流向均值，中位数，百分位数，时效仍然是用的od_tm，但只统计仓库发出的数据
--将时效分位数用list的形式展示
create table if not exists tmp_dm_as.cxy_new_biaokuai_warehouse_per_perget stored as parquet as
    select product_code as flow_product_code
           ,src_dist_code
           ,src_city_code
           ,dest_dist_code
           ,dest_city_code
           ,count(*) as wh_waybill_num
           ,avg(od_tm) as wh_avg_od_tm
           ,percentile_approx(od_tm,0.05,3000) od_tm_05
           ,percentile_approx(od_tm,0.25,3000) od_tm_25
           ,percentile_approx(od_tm,0.50,3000) od_tm_mid
           ,percentile_approx(od_tm,0.75,3000) od_tm_75
           ,percentile_approx(od_tm,0.95,3000) od_tm_95
           ,sum(case when od_tm<=24 then 1 else 0 end)*1.0/count(*) wh_intm_24_rt
           ,sum(case when od_tm<=48 then 1 else 0 end)*1.0/count(*) wh_intm_48_rt
           ,sum(case when od_tm<=72 then 1 else 0 end)*1.0/count(*) wh_intm_72_rt
        from tmp_dm_as.cxy_new_biaokuai_warehouse_od a
        where  error_type is null
          and whl_error_type is null
          and od_tm>0
        group by product_code,src_dist_code,src_city_code,dest_dist_code,dest_city_code;

--仓库各流向众数
create table if not exists tmp_dm_as.cxy_new_biaokuai_warehouse_tm_mode stored as parquet as 
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
                    from tmp_dm_as.cxy_new_biaokuai_warehouse_od a
                    where error_type is null
                      and whl_error_type is null
                      and od_tm>0
                    group by product_code,src_dist_code,src_city_code,dest_dist_code,dest_city_code,round(od_tm,0)  
                    ) a 
            ) b 
        where b.rn = 1 ;


--时效合并
--限定至少有10单，才能有效
create table tmp_dm_as.cxy_new_biaokuai_tm_flowbase  stored as parquet as
select a.flow_product_code
      ,a.src_dist_code
      ,a.src_city_code
      ,a.dest_dist_code
      ,a.dest_city_code
      ,a.waybill_num
      ,a.avg_od_tm
      ,a.od_tm_05
      ,a.od_tm_25
      ,a.od_tm_mid
      ,a.od_tm_75
      ,a.od_tm_95
      ,a.intm_24_num
      ,a.intm_48_num
      ,a.intm_72_num
      ,a.intm_that_num
      ,a.intm_next_num
      ,a.intm_other_num
      ,b.od_tm_mode
  from 
       (select *
          from tmp_dm_as.cxy_new_biaokuai_per_perget  a  --物流产品时效分位数计算
          where waybill_num>=10
       ) a

       join

       tmp_dm_as.cxy_new_biaokuai_tm_mode b   --物流产品时效众数计算
       on a.flow_product_code=b.flow_product_code and a.src_dist_code = b.src_dist_code and a.dest_dist_code = b.dest_dist_code;


---------------------------------------------------------------------------
--补充数据准备
--关联流向全表，获得省，距离相关数据
--编码也关联得到价格编码，也就是price_product_code
drop table tmp_dm_as.cxy_new_biaokuai_tm_flowbase_all_tmp;
create table tmp_dm_as.cxy_new_biaokuai_tm_flowbase_all_tmp stored as parquet as
select a.price_product_code product_code       --价格的代码为唯一代码
      ,a.price_product_name product_name 
      ,a.flow_product_code
      ,a.src_code,a.src_city,a.src_province,a.src_dist,a.src_area,a.dest_code,a.dest_city,a.dest_province,a.dest_dist,a.dest_area,a.area_type,a.distance,a.distance_section
      ,b.waybill_num,b.avg_od_tm,b.od_tm_mode,b.od_tm_05,b.od_tm_25,b.od_tm_mid,b.od_tm_75,b.od_tm_95
      ,case when b.waybill_num>0 then b.intm_24_num*1.0/b.waybill_num else cast(null as double) end as intm_24_rt
      ,case when b.waybill_num>0 then b.intm_48_num*1.0/b.waybill_num else cast(null as double) end as intm_48_rt
      ,case when b.waybill_num>0 then b.intm_72_num*1.0/b.waybill_num else cast(null as double) end as intm_72_rt
      ,case when b.waybill_num>0 then b.intm_that_num*1.0/b.waybill_num else cast(null as double) end as intm_that_rt
      ,case when b.waybill_num>0 then b.intm_next_num*1.0/b.waybill_num else cast(null as double) end as intm_next_rt
      ,case when b.waybill_num>0 then b.intm_other_num*1.0/b.waybill_num else cast(null as double) end as intm_other_rt
  from 
       (select * from dm_as.ts_supply_product_flow_all_info 
       where flow_product_code='SE0001') a       ---基础流向表
     
       left join 

       tmp_dm_as.cxy_new_biaokuai_tm_flowbase b on a.src_code = b.src_dist_code and a.dest_code = b.dest_dist_code and a.flow_product_code=b.flow_product_code;

--对于没有时效数据的流向，用产品+省份+距离区间维度的均值来替代，如果此均值也没有，则用产品+距离区间均值替代
--下面开始填补
--填补数据准备
--各产品+省+距离均值
drop table tmp_dm_as.cxy_new_biaokuai_tm_flow_province;
create table tmp_dm_as.cxy_new_biaokuai_tm_flow_province stored as parquet as
select product_code 
      ,src_province
      ,dest_province
      ,distance_section
      ,avg(avg_od_tm) as avg_od_tm
      ,avg(od_tm_mode) as od_tm_mode
      ,avg(od_tm_05) as od_tm_05
      ,avg(od_tm_25) as od_tm_25
      ,avg(od_tm_mid) as od_tm_mid
      ,avg(od_tm_75) as od_tm_75
      ,avg(od_tm_95) as od_tm_95
      ,sum(intm_24_rt*waybill_num)*1.0/sum(waybill_num) as intm_24_rt 
      ,sum(intm_48_rt*waybill_num)*1.0/sum(waybill_num) as intm_48_rt 
      ,sum(intm_72_rt*waybill_num)*1.0/sum(waybill_num) as intm_72_rt 
      ,sum(intm_that_rt*waybill_num)*1.0/sum(waybill_num) as intm_that_rt 
      ,sum(intm_next_rt*waybill_num)*1.0/sum(waybill_num) as intm_next_rt 
      ,sum(intm_other_rt*waybill_num)*1.0/sum(waybill_num) as intm_other_rt 
      ,sum(waybill_num) as waybill_num
  from tmp_dm_as.cxy_new_biaokuai_tm_flowbase_all_tmp
 where waybill_num>=10
 group by product_code,src_province,dest_province,distance_section;


 --各产品+距离均值 --标快所有距离区间都有
 drop table tmp_dm_as.cxy_new_biaokuai_flow_distance_pure;
create table tmp_dm_as.cxy_new_biaokuai_flow_distance_pure as
select product_code
      ,distance_section
      ,avg(avg_od_tm) as avg_od_tm
      ,avg(od_tm_mode) as od_tm_mode
      ,avg(od_tm_05) as od_tm_05
      ,avg(od_tm_25) as od_tm_25
      ,avg(od_tm_mid) as od_tm_mid
      ,avg(od_tm_75) as od_tm_75
      ,avg(od_tm_95) as od_tm_95
      ,sum(intm_24_rt*waybill_num)*1.0/sum(waybill_num) as intm_24_rt 
      ,sum(intm_48_rt*waybill_num)*1.0/sum(waybill_num) as intm_48_rt 
      ,sum(intm_72_rt*waybill_num)*1.0/sum(waybill_num) as intm_72_rt 
      ,sum(intm_that_rt*waybill_num)*1.0/sum(waybill_num) as intm_that_rt 
      ,sum(intm_next_rt*waybill_num)*1.0/sum(waybill_num) as intm_next_rt 
      ,sum(intm_other_rt*waybill_num)*1.0/sum(waybill_num) as intm_other_rt 
      ,sum(waybill_num) waybill_num
  from tmp_dm_as.cxy_new_biaokuai_tm_flowbase_all_tmp
 where waybill_num>=10 
 group by product_code,distance_section;

---还是模拟演示一下距离的拟合填补
--简单用系数拟合了，鉴于网点间距离不会特别远，所以没有特别严格处理
drop table tmp_dm_as.cxy_new_biaokuai_flow_distance_pure_test;
create table tmp_dm_as.cxy_new_biaokuai_flow_distance_pure_test stored as parquet as
select a.price_product_code product_code
      ,a.distance_section
      ,nvl(b.avg_od_tm,c.avg_od_tm ) as avg_od_tm
      ,nvl(b.od_tm_mode,c.od_tm_mode ) as od_tm_mode
      ,nvl(b.od_tm_05,c.od_tm_05) as od_tm_05
      ,nvl(b.od_tm_25,c.od_tm_25) as od_tm_25
      ,nvl(b.od_tm_mid,c.od_tm_mid) as od_tm_mid
      ,nvl(b.od_tm_75,c.od_tm_75) as od_tm_75
      ,nvl(b.od_tm_95,c.od_tm_95) as od_tm_95
      ,nvl(b.intm_24_rt,c.intm_24_rt) as intm_24_rt 
      ,nvl(b.intm_48_rt,c.intm_48_rt) as intm_48_rt 
      ,nvl(b.intm_72_rt,c.intm_72_rt) as intm_72_rt 
      ,nvl(b.intm_that_rt,c.intm_that_rt) as intm_that_rt 
      ,nvl(b.intm_next_rt,c.intm_next_rt) as intm_next_rt 
      ,nvl(b.intm_other_rt,c.intm_other_rt) as intm_other_rt
      ,nvl(b.waybill_num,c.waybill_num ) as waybill_num
      ,case when b.avg_od_tm is null then 1 else 0 end add_mark_distance
      from
     (select price_product_code
              ,distance_section
              ,case when distance_section like '%6000km%' then '>6000km'     --不知道什么原因匹配不上，只能这样操作
                    else distance_section end join_distance_section
          from dm_as.ts_supply_distance_section_info 
          where price_product_code='S1') a 
    left join 
    tmp_dm_as.cxy_new_biaokuai_flow_distance_pure b 
    on a.price_product_code=b.product_code and a.distance_section=b.distance_section
    left join 
     (select product_code
              ,'>6000km' distance_section
              ,avg_od_tm*1.133 avg_od_tm
              ,od_tm_mode*1.145 od_tm_mode      --众数比较波动，选择中位数的系数
              ,od_tm_05*1.225 as od_tm_05
              ,od_tm_25*1.157 as od_tm_25
              ,od_tm_mid*1.145 as od_tm_mid
              ,od_tm_75*1.137 as od_tm_75
              ,od_tm_95*1.089 as od_tm_95
              ,0.0 as intm_24_rt    --按照特惠的标准，72小时无法送达
              ,0.0 as intm_48_rt
              ,0.0 as intm_72_rt
              ,0.0 as intm_that_rt
              ,0.0 as intm_next_rt
              ,0.0 as intm_other_rt
              ,waybill_num
          from tmp_dm_as.cxy_new_biaokuai_flow_distance_pure
         where distance_section='(5000-6000]'
       ) c on a.price_product_code=c.product_code and a.join_distance_section=c.distance_section

drop table tmp_dm_as.cxy_new_biaokuai_tm_flow_after_tianbu;
create table tmp_dm_as.cxy_new_biaokuai_tm_flow_after_tianbu stored as parquet as
select a.product_code,a.product_name,a.flow_product_code,a.src_code,a.src_city,a.src_province,a.src_dist,a.src_area,a.dest_code,a.dest_city,
       a.dest_province,a.dest_dist,a.dest_area,a.area_type,a.distance,a.distance_section
      ,case when a.avg_od_tm is null then 1 else 0 end as add_mark
      ,nvl(nvl(a.avg_od_tm  ,b.avg_od_tm  ), c.avg_od_tm  )  as avg_od_tm
      ,nvl(nvl(a.od_tm_mode ,b.od_tm_mode ), c.od_tm_mode )  as od_tm_mode
      ,nvl(nvl(a.od_tm_05,b.od_tm_05),c.od_tm_05) as od_tm_05
      ,nvl(nvl(a.od_tm_25,b.od_tm_25),c.od_tm_25) as od_tm_25
      ,nvl(nvl(a.od_tm_mid,b.od_tm_mid),c.od_tm_mid) as od_tm_mid
      ,nvl(nvl(a.od_tm_75,b.od_tm_75),c.od_tm_75) as od_tm_75
      ,nvl(nvl(a.od_tm_95,b.od_tm_95),c.od_tm_95) as od_tm_95
      ,nvl(nvl(a.intm_24_rt,b.intm_24_rt),c.intm_24_rt) as intm_24_rt 
      ,nvl(nvl(a.intm_48_rt,b.intm_48_rt),c.intm_48_rt) as intm_48_rt 
      ,nvl(nvl(a.intm_72_rt,b.intm_72_rt),c.intm_72_rt) as intm_72_rt 
      ,nvl(nvl(a.intm_that_rt,b.intm_that_rt),c.intm_that_rt) as intm_that_rt 
      ,nvl(nvl(a.intm_next_rt,b.intm_next_rt),c.intm_next_rt) as intm_next_rt 
      ,nvl(nvl(a.intm_other_rt,b.intm_other_rt),c.intm_other_rt) as intm_other_rt
      ,nvl(nvl(a.waybill_num,b.waybill_num), c.waybill_num)  as waybill_num
  from 
       tmp_dm_as.cxy_new_biaokuai_tm_flowbase_all_tmp a
       
       left join 

       tmp_dm_as.cxy_new_biaokuai_tm_flow_province b on a.product_code=b.product_code and a.src_province=b.src_province and a.dest_province=b.dest_province and a.distance_section=b.distance_section
       
       left join 

       tmp_dm_as.cxy_new_biaokuai_flow_distance_pure_test c on a.product_code=c.product_code and a.distance_section=c.distance_section;

--汇总一般时效和仓的时效
drop table tmp_dm_as.cxy_new_biaokuai_flow_tm_sixmonth;
create table tmp_dm_as.cxy_new_biaokuai_flow_tm_sixmonth as
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
      ,b.od_tm_05 as wh_od_tm_05
      ,b.od_tm_mid as wh_od_tm_mid
      ,b.od_tm_95 as wh_od_tm_95
      ,c.wh_od_tm_mode
      ,b.od_tm_25 as wh_od_tm_25
      ,b.od_tm_75 as wh_od_tm_75
      ,b.wh_intm_24_rt
      ,b.wh_intm_48_rt
      ,b.wh_intm_72_rt
      ,b.wh_waybill_num
      ,a.flow_product_code
      ,a.intm_that_rt 
      ,a.intm_next_rt 
      ,a.intm_other_rt
  from 
       tmp_dm_as.cxy_new_biaokuai_tm_flow_after_tianbu a  --填补完整后的流向时效数据
       
       left outer join 
       
       (select case when flow_product_code='S1' then 'SE0001'
               else flow_product_code end flow_product_code_,*
          from tmp_dm_as.cxy_new_biaokuai_warehouse_per_perget a  --仓库发货的流向时效数据
         where wh_waybill_num>=10
       ) b on a.flow_product_code=b.flow_product_code_ and a.src_code=b.src_dist_code and a.dest_code=b.dest_dist_code
       
       left outer join 
       (select case when flow_product_code='S1' then 'SE0001'
               else flow_product_code end flow_product_code_,*
       from tmp_dm_as.cxy_new_biaokuai_warehouse_tm_mode) c --仓库发货的流向众数数据
       on a.flow_product_code=c.flow_product_code_ and a.src_code=c.src_dist_code and a.dest_code=c.dest_dist_code;


--时效价格合并-城市代码唯一
drop table tmp_dm_as.cxy_new_biaokuai_org_city_code_odtm_price;
create table if not exists tmp_dm_as.cxy_new_biaokuai_org_city_code_odtm_price as 
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
      ,a.intm_that_rt 
      ,a.intm_next_rt 
      ,a.intm_other_rt
  from 
       (select *           
          from tmp_dm_as.cxy_new_biaokuai_flow_tm_sixmonth  --合并所有流向和从仓库发货的流向的时效数据
       ) a
       
       left join 

       (select *
          from tmp_dm_as.cxy_new_biaokuai_flow_price  --产品流向对应的价格
       ) b on a.product_code=b.product_code and a.src_code=b.src_code and a.dest_code=b.dest_code;

drop table tmp_dm_as.cxy_new_biaokuai_org_city_name_odtm_price;
create table if not exists tmp_dm_as.cxy_new_biaokuai_org_city_name_odtm_price as 
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
      ,intm_that_rt 
      ,intm_next_rt 
      ,intm_other_rt
  from 
       (select b.*,deal_src_city
          from 
               (select a.*,deal_dest_city
                  from 
                       (select *
                          from tmp_dm_as.cxy_new_biaokuai_org_city_code_odtm_price a 
                       ) a
                      LATERAL VIEW explode(split(dest_city,'\\/')) cc AS deal_dest_city 
               ) b 
               LATERAL VIEW explode(split(src_city,'\\/')) cc AS deal_src_city 
       ) t;


--异常处理
--1.非同城运输，中位数时长小于0.1的
create table tmp_dm_as.cxy_new_biaokuai_city_od_error_flow as
select product_code
      ,product_name
      ,src_code
      ,src_city
      ,dest_code
      ,dest_city
      ,'时效中位数太小' error_type
  from tmp_dm_as.cxy_new_biaokuai_org_city_name_odtm_price a
 where distance_section not like '%同城%'
   and od_tm_mid<=0.1;

--2.时长相比同省临近3个城市，小50%，且不是二线以上的城市，则认为是异常
--其中：针对多个城市属于同一个编码，如果某城市归属于二线以上的城市，则认为这个城市也是二线以上的
--3.时长相比同省临近3个城市，大50%，且是系统补足数据，则认为异常
insert into table tmp_dm_as.cxy_new_biaokuai_city_od_error_flow
select product_code,product_name,src_code,src_city,dest_code,dest_city,error_type
from
(select product_code,product_name,src_code,src_city,dest_code,dest_city,add_mark
              ,case when od_tm_mid/avg_near_od_tm_mid-1>0.5 and add_mark=1 then '时效超过周边流向' 
                    when od_tm_mid/avg_near_od_tm_mid-1<-0.5 and src_city not in ('北京市','常州市','成都市','大连市','东莞市','佛山市','福州市','广州市','贵阳市','哈尔滨市','海口市','杭州市','合肥市','呼和浩特市','惠州市','济南市','嘉兴市','金华市','昆明市','拉萨市','兰州市','南昌市','南京市','南宁市','南通市','宁波市','青岛市','泉州市','厦门市','上海市','绍兴市','深圳市','沈阳市','石家庄市','苏州市','台州市','太原市','天津市','温州市','乌鲁木齐市','无锡市','武汉市','西安市','西宁市','徐州市','烟台市','扬州市','银川市','长春市','长沙市','郑州市','中山市','重庆市','珠海市','崇左市','眉山市','资阳市','抚顺市','铁岭市','咸阳市') 
                         then '时效低于周边流向' --一二线城市和省会城市的运输本身就比较快，可以不用考虑时效低的情况
                    else '' end error_type
          from
(select product_code,product_name,src_code,src_city,dest_code,dest_city,add_mark,od_tm_mid
                      ,avg(case when rank_same_province<=3 then near_od_tm_mid else null end) as avg_near_od_tm_mid
                  from 
(select aa.product_code,aa.product_name,aa.src_code,aa.src_city,aa.dest_code,aa.dest_city,aa.od_tm_mid,aa.add_mark,b.near_od_tm_mid
                              ,row_number() over(PARTITION BY aa.product_code,aa.src_city,aa.dest_city ORDER BY rank_same_province) as rank_same_province --重新排序，确保取到的周边城市的rank是连续的
                          from 
(select a.product_code,a.product_name,a.src_code,a.src_city,a.dest_code,a.dest_city,a.od_tm_mid,a.add_mark
                                      ,b.near_src_city,b.rank_same_province
from  
（select product_code,product_name,src_code,src_city,dest_code,dest_city,od_tm_mid,add_mark
                                          from tmp_dm_as.cxy_new_biaokuai_org_city_name_odtm_price a  --合并时效和价格并拆分城市后的结果表
                                         where od_tm_mid>0.1
                                           and distance_section not like '%同城%'
                                         group by product_code,product_name,src_code,src_city,dest_code,dest_city,od_tm_mid,add_mark) a 
 join 

                                       (select sf_city
                                              ,near_sf_city as near_src_city
                                              ,rank_same_province
                                          from dm_as.ts_supply_sfcity_nearby_rank  --顺丰城市及其周边城市
                                       ) b on a.src_city=b.sf_city 
                                                                        ) aa
join 
 (select product_code,product_name,src_code,src_city,dest_code,dest_city
                                       ,od_tm_mid as near_od_tm_mid
                                  from dm_as.ts_supply_org_city_name_od_tm a
                                  where od_tm_mid>0.1
                                    and distance_section not like '%同城%'
                                  group by product_code,product_name,src_code,src_city,dest_code,dest_city,od_tm_mid
                                ) b on aa.product_code=b.product_code and aa.near_src_city=b.src_city and aa.dest_city=b.dest_city) t1 
                                            group by product_code,product_name,src_code,src_city,dest_code,dest_city,add_mark,od_tm_mid) t2 
                                                                                                                                               ) aaa
where not error_type='';

--异常流向替代表
create table tmp_dm_as.cxy_new_biaokuai_city_od_error_flow_replace as 
select a.product_code,a.product_name,a.src_code,a.src_city,a.dest_code,a.dest_city,a.error_type,a.new_src_city
       ,b.avg_od_tm,b.od_tm_05,b.od_tm_mid,b.od_tm_95,b.od_tm_mode,b.od_tm_25,b.od_tm_75,b.intm_24_rt,b.intm_48_rt,b.intm_72_rt --用新的数据替换
       ,intm_that_rt,intm_next_rt,intm_other_rt
       from
(select product_code,product_name,src_code,src_city,dest_code,dest_city,error_type,new_src_city
          from 
(select aa.product_code,aa.product_name,aa.src_code,aa.src_city,aa.dest_code,aa.dest_city,aa.error_type,aa.new_src_city
                      ,row_number() over(PARTITION BY aa.product_code,aa.src_city,aa.dest_city ORDER BY rank_same_province) as rank_same_province 
                  from
(select a.product_code,a.product_name,a.src_code,a.src_city,a.dest_code,a.dest_city,a.error_type  --关联最近的城市
                              ,b.new_src_city,b.rank_same_province
                          from
                          (select *
                                  from dm_as.ts_supply_city_od_error_flow_tm a
                                ) a
                                
                                join 

                                (select sf_city,near_sf_city as new_src_city,rank_same_province
                                   from dm_as.ts_supply_sfcity_nearby_rank
                                ) b on a.src_city=b.sf_city
                                where  b.new_src_city != a.dest_city) aa --剔除同城数据做替换 
left join 
 (select *
                          from dm_as.ts_supply_city_od_error_flow_tm a
                       ) bb on aa.product_name=bb.product_name and aa.new_src_city=bb.src_city and aa.dest_city=bb.dest_city
where bb.src_city is null  --剔除已经异常的流向，不作为替换数据
                                                             ) aaa
where rank_same_province=1 
                                ) a

join 

       (select product_code,product_name,src_city,dest_city
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
              ,max(intm_that_rt) as intm_that_rt 
              ,max(intm_next_rt) as intm_next_rt 
              ,max(intm_other_rt) as intm_other_rt
          from tmp_dm_as.cxy_new_biaokuai_org_city_name_odtm_price a
         group by product_code,product_name,src_city,dest_city
       ) b on a.product_code=b.product_code and a.product_name=b.product_name and a.new_src_city=b.src_city and a.dest_city=b.dest_city;

--清洗后的流向时效表
--将异常的流向用正常的时效替代
drop table tmp_dm_as.cxy_new_biaokuai_city_od_price_pure;
create table tmp_dm_as.cxy_new_biaokuai_city_od_price_pure as 
select /*+mapjoin(b)*/ a.src_code,a.src_city,a.src_province,a.src_dist,a.src_area,a.dest_code,a.dest_city,a.dest_province,a.dest_dist,a.dest_area,a.distance,a.distance_section,a.product_code,a.product_name
       ,case when b.src_city is null then a.add_mark else 2 end add_mark
       ,case when b.src_city is null then a.avg_od_tm else b.avg_od_tm end avg_od_tm
       ,case when b.src_city is null then a.od_tm_05 else b.od_tm_05 end od_tm_05
       ,case when b.src_city is null then a.od_tm_mid else b.od_tm_mid end od_tm_mid
       ,case when b.src_city is null then a.od_tm_95 else b.od_tm_95 end od_tm_95
       ,case when b.src_city is null then a.od_tm_mode else b.od_tm_mode end od_tm_mode
       ,a.pay_country,a.price_code,a.base_weight_qty,a.base_price,a.weight_price_qty,a.max_weight_qty,a.light_factor,a.od_normal
       ,case when b.src_city is null then a.od_tm_25 else b.od_tm_25 end od_tm_25
       ,case when b.src_city is null then a.od_tm_75 else b.od_tm_75 end od_tm_75
       ,case when b.src_city is null then a.intm_24_rt else b.intm_24_rt end intm_24_rt
       ,case when b.src_city is null then a.intm_48_rt else b.intm_48_rt end intm_48_rt
       ,case when b.src_city is null then a.intm_72_rt else b.intm_72_rt end intm_72_rt
       ,a.wh_avg_od_tm,a.wh_od_tm_05,a.wh_od_tm_mid,a.wh_od_tm_95,a.wh_od_tm_mode,a.wh_od_tm_25,a.wh_od_tm_75,a.wh_intm_24_rt,a.wh_intm_48_rt,a.wh_intm_72_rt --顺丰仓的数据不进行替换
       ,cast(1 as int) product_is_valid
       ,cast(1 as int) product_user_type
       ,case when b.src_city is null then a.intm_that_rt else b.intm_that_rt end as intm_that_rt  
       ,case when b.src_city is null then a.intm_next_rt else b.intm_next_rt end as intm_next_rt  
       ,case when b.src_city is null then a.intm_other_rt else b.intm_other_rt end as intm_other_rt 
  from 
       (select *
          from tmp_dm_as.cxy_new_biaokuai_org_city_name_odtm_price a        --原始的时效数据
       ) a
       
       left outer join 

       (select *
          from tmp_dm_as.cxy_new_biaokuai_city_od_error_flow_replace a        --异常流向替代时效表
        ) b on a.product_code=b.product_code and a.src_city=b.src_city and a.dest_city=b.dest_city;

--按照城市编码合并，给线上使用
create table tmp_dm_as.cxy_new_biaokuai_code_od_price_pure as 
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
           ,max(intm_that_rt) as intm_that_rt 
           ,max(intm_next_rt) as intm_next_rt 
           ,max(intm_other_rt) as intm_other_rt
      from tmp_dm_as.cxy_new_biaokuai_city_od_price_pure a
      group by src_code,dest_code,product_code,base_weight_qty


--放入dm_as库
drop table dm_as.cxy_new_biaokuai_city_name_odtm_price;
create Table if not exists dm_as.cxy_new_biaokuai_city_name_odtm_price
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
    ,intm_that_rt double comment '当日达成率'
    ,intm_next_rt double comment '次日达成率'
    ,ntm_other_rt double comment '隔日达成率'
)comment '顺丰标快2020年2月-8月时效价格表（城市名唯一)'
partitioned by (inc_month string);


insert overwrite table dm_as.cxy_new_biaokuai_city_name_odtm_price partition(inc_month='202008')
select * from tmp_dm_as.cxy_new_biaokuai_city_od_price_pure;


create Table if not exists dm_as.cxy_new_biaokuai_city_code_odtm_price
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
    ,intm_that_rt double comment '当日达成率'
    ,intm_next_rt double comment '次日达成率'
    ,ntm_other_rt double comment '隔日达成率'
)comment '顺丰标快2020年2月-8月时效价格表（城市编码唯一)'
partitioned by (inc_month string);

insert overwrite table dm_as.cxy_new_biaokuai_city_code_odtm_price partition(inc_month='202008')
select * from tmp_dm_as.cxy_new_biaokuai_code_od_price_pure;

---不能用这种方式，因为在用临近城市替换的时候，同一个的city_code的不同城市的最邻近城市可能不同
create table tmp_dm_as.cxy_new_biaokuai_code_od_price_pure_duibi as 
select src_code
           ,concat_ws("/",sort_array(collect_set(src_city))) src_city
           ,src_province
           ,src_dist
           ,src_area
           ,dest_code
           ,concat_ws("/",sort_array(collect_set(dest_city))) dest_city
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
           ,product_is_valid
           ,product_user_type
           ,intm_that_rt 
           ,intm_next_rt 
           ,intm_other_rt
      from tmp_dm_as.cxy_new_biaokuai_city_od_price_pure a
      group by src_code
           ,src_province
           ,src_dist
           ,src_area
           ,dest_code
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
           ,product_is_valid
           ,product_user_type
           ,intm_that_rt 
           ,intm_next_rt 
           ,intm_other_rt