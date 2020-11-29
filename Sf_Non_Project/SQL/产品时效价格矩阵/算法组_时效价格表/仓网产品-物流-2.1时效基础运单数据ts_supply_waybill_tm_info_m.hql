--参数配置
set hive.exec.reducers.max=499;

--建表
create table if not exists dm_as.ts_supply_waybill_tm_info_m
(
    waybill_no string comment '运单号'
    ,source_zone_code string comment '原寄地网点代码'
    ,dest_zone_code string comment '目的地网点代码，未派送前是城市代码'
    ,src_dist_code string comment '原寄地地区代码'
    ,src_city_code string comment '原寄地城市代码'
    ,src_county string comment '原寄地区县'
    ,dest_dist_code  string comment '目的地地区代码'
    ,dest_city_code string comment '目的地城市代码'
    ,dest_county string comment '目的地区县'
    ,product_code string comment '产品代码'
    ,freight_monthly_acct_code string comment '运费月结账号'
    ,cod_monthly_acct_code string comment '代收款月结账号'
    ,inc_day string comment '订单时间'
    ,od_tm_org double comment '原始时效(h)'
    ,od_tm double comment '时效(h)'
    ,consigned_tm string comment '发货时间'
    ,signin_tm string comment '收货时间'
    ,error_type string comment '异常类型，为null则没有异常'
    ,handover_bill string comment '是否放入丰巢柜,或交接给合作点'
    ,barscantm string comment '放入丰巢柜,或交接给合作点时间'
)comment '仓网产品-运单中间表'  partitioned by (inc_month string) stored as parquet;

create table if not exists dm_as.ts_supply_waybill_whl_tm_m
(
    waybill_no string comment '运单号'
    ,source_zone_code string comment '原寄地网点代码'
    ,dest_zone_code string comment '目的地网点代码，未派送前是城市代码'
    ,src_dist_code string comment '原寄地地区代码'
    ,src_city_code string comment '原寄地城市代码'
    ,dest_dist_code  string comment '目的地地区代码'
    ,dest_city_code string comment '目的地城市代码'
    ,product_code string comment '产品代码'
    ,freight_monthly_acct_code string comment '运费月结账号'
    ,cod_monthly_acct_code string comment '代收款月结账号'
    ,inc_day string comment '订单时间'
    ,od_tm_org double comment '原始时效(h)'
    ,od_tm double comment '时效(h)'
    ,consigned_tm string comment '发货时间'
    ,signin_tm string comment '收货时间'
    ,error_type string comment '异常类型，为null则没有异常'
    ,handover_bill string comment '是否放入丰巢柜,或交接给合作点'
    ,barscantm string comment '放入丰巢柜,或交接给合作点时间'
    ,warehouse_code string comment ''
    ,warehouse_name string comment ''
    ,carrier_serviece_name string comment ''
    ,actual_shipment_date string comment ''
    ,whl_error_type string comment ''
)comment '仓网产品-仓库运单中间表' partitioned by (inc_month string) stored as parquet;

--数据量级较大，后续考虑只保留1个月
alter table dm_as.ts_supply_waybill_tm_info_m drop partition (inc_month = '$[month(yyyyMM,-2)]');
alter table dm_as.ts_supply_waybill_whl_tm_m drop partition (inc_month = '$[month(yyyyMM,-2)]');

--基础时效订单数据
----od_tm_org为原始的时效
----od_tm为处理后时效：如果放入丰巢柜,或交接给合作点，且订单不是转寄退回的，且巴枪时间早于送达时间，则用巴枪时间作为最后送达时间，计算时效
----标识异常订单：
------最后派件异常时间 存在异常
------同城48小时未送达，识别为异常，仅'SE0004','SE0118','SE0117','SE0001','SE0003'几个产品
------转寄和退回的单，识别为异常
------双十一期间的订单

drop table if exists tmp_dm_as.ts_supply_waybill_abnormal_tmp ;
create table if not exists tmp_dm_as.ts_supply_waybill_abnormal_tmp stored as parquet as 
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
              where inc_day between '$[day(yyyyMMdd,-120)]' and '$[day(yyyyMMdd,-0)]'
                and opcode in ('99','870','125','657')
           ) a 
      where rnk=1;
              
--存在waybill_no重复数据，占比0.003%，不明原因暂未处理
alter table dm_as.ts_supply_waybill_tm_info_m drop partition (inc_month = '$[month(yyyyMM,-1)]');
insert overwrite table dm_as.ts_supply_waybill_tm_info_m partition (inc_month = '$[month(yyyyMM,-1)]')
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
                when source_zone_code = dest_zone_code and product_code in ('SE0004','SE0118','SE0117','SE0001','SE0003') and od_tm_org>=48 then '同地区超过48小时'
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
              where inc_day between '$[day(yyyyMMdd,-120)]' and '$[day(yyyyMMdd,-0)]'
                and src_dist_code is not null       --剔除信息异常的数据
                and dest_dist_code is not null
                and product_code is not null
              group by waybill_no,source_zone_code,dest_zone_code,src_dist_code,src_city_code,src_county,dest_dist_code,dest_city_code,dest_county,product_code,freight_monthly_acct_code,cod_monthly_acct_code,nvl(substr(recv_bar_tm,1,19),consigned_tm),nvl(substr(send_bar_tm,1,19),signin_tm),inc_day
            ) a
      left outer join tmp_dm_as.ts_supply_waybill_abnormal_tmp b on a.waybill_no=b.mainwaybillno;
            
        
--顺丰仓订单时效数据
drop table if exists tmp_dm_as.ts_supply_waybill_whl_od_tm_tmp ;
create table if not exists tmp_dm_as.ts_supply_waybill_whl_od_tm_tmp stored as parquet as 
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
              where inc_day between '$[day(yyyyMMdd,-120)]' and '$[day(yyyyMMdd,-1)]'
            )  t 
        where rnk = 1;


--关联运单表，作为从仓发货的基础数据
alter table dm_as.ts_supply_waybill_whl_tm_m drop partition (inc_month = '$[month(yyyyMM,-1)]');
insert overwrite table dm_as.ts_supply_waybill_whl_tm_m partition (inc_month = '$[month(yyyyMM,-1)]')
    select a.waybill_no,a.source_zone_code,a.dest_zone_code,a.src_dist_code,a.src_city_code,a.dest_dist_code,a.dest_city_code,a.product_code,a.freight_monthly_acct_code,a.cod_monthly_acct_code,a.inc_day,a.od_tm_org,a.od_tm,a.consigned_tm,a.signin_tm,a.error_type,a.handover_bill,a.barscantm
           ,b.warehouse_code,b.warehouse_name,b.carrier_serviece_name,b.actual_shipment_date,b.error_type whl_error_type
    from (select *
            from dm_as.ts_supply_waybill_tm_info_m a
            where inc_month = '$[month(yyyyMM,-1)]'
         ) a 
    join tmp_dm_as.ts_supply_waybill_whl_od_tm_tmp b 
        on a.waybill_no = b.waybill_no
    where warehouse_code is not null ;

--清除不需要的数据
drop table tmp_dm_as.ts_supply_waybill_abnormal_tmp;
drop table tmp_dm_as.ts_supply_waybill_whl_od_tm_tmp;

