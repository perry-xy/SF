--参数配置
--set hive.execution.engine=tez;

--价格数据整合
--产品价格表
create Table if not exists dm_as.ts_supply_product_flow_price_m
(
    product_code string comment '产品代码'
    ,product_name string comment '产品代码'
    ,src_code string comment '出发城市编码'
    ,src_name string comment '出发城市名称'
    ,dest_code string comment '目的城市编码'
    ,dest_name string comment '目的城市名称'
    ,pay_country string comment '付款地国家'
    ,price_code string comment '价格区码'
    ,base_weight_qty double comment '首重'
    ,base_price double comment '首重运费'
    ,weight_price_qty double comment '续重运费'
    ,max_weight_qty double comment '最大重量'
    ,light_factor double comment '轻抛'
    ,od_normal int comment '该流向是否有对应产品价格信息'
)comment '产品流向价格数据'
partitioned by (inc_month string);

--调整冷运零担的价格计算逻辑，按照单价和一口价取价高者
insert overwrite table dm_as.ts_supply_product_cold_weight_m partition (inc_month = '$[month(yyyyMM,-1)]' )
select product_name,product_code,source_code,source_name,dest_code,dest_name,pay_country,price_code,base_weight_qty,base_price,weight_price_qty,max_weight_qty,light_factor
      ,case when weight_price_qty='0.0' then max_weight_qty else Round(base_price/weight_price_qty,1) end as weigth_qty
  from dm_as.joanna_price_base_all a
 where inc_day ='$[day(yyyyMMdd,-1)]' 
   and flow_type = '1'
   and base_weight_qty>=0
   and base_price>=0
   and weight_price_qty>=0
   and max_weight_qty>=0
   and light_factor>=0 
   and product_code='SE0030';  --冷运零担

--将跨越了weigth_qty的区间挑出来分成两个区间，其他的按照逻辑修改
insert overwrite table dm_as.ts_supply_product_cold_price_m partition (inc_month = '$[month(yyyyMM,-1)]' )
select product_name,product_code,source_code,source_name,dest_code,dest_name,pay_country,price_code
      ,min(cast(base_weight_qty as double)) as base_weight_qty
      ,base_price,weight_price_qty
      ,max(cast(max_weight_qty as double)) as max_weight_qty
      ,light_factor
  from 
       (select product_name,product_code,source_code,source_name,dest_code,dest_name,pay_country,price_code
              ,base_weight_qty
              ,base_price
              ,'0' as weight_price_qty
              ,weigth_qty as max_weight_qty
              ,light_factor
          from dm_as.ts_supply_product_cold_weight_m
         where inc_month='$[month(yyyyMM,-1)]'
           and cast(weigth_qty as double)>cast(base_weight_qty as double)
           and cast(weigth_qty as double)<cast(max_weight_qty as double)

        union all 

        select product_name,product_code,source_code,source_name,dest_code,dest_name,pay_country,price_code
              ,case when cast(weigth_qty as double)>cast(base_weight_qty as double) and cast(weigth_qty as double)<cast(max_weight_qty as double) then weigth_qty else base_weight_qty end as base_weight_qty
              ,case when cast(weigth_qty as double)>=cast(max_weight_qty as double) then base_price else '0' end as base_price
              ,case when cast(weigth_qty as double)>=cast(max_weight_qty as double) then '0' else weight_price_qty end as weight_price_qty
              ,max_weight_qty,light_factor
          from dm_as.ts_supply_product_cold_weight_m
         where inc_month='$[month(yyyyMM,-1)]'
       ) t
 group by product_name,product_code,source_code,source_name,dest_code,dest_name,pay_country,price_code,base_price,weight_price_qty,light_factor
 order by source_code,dest_code,cast(base_weight_qty as double);



--计算需要产品和流向对应价格
--1.从配置表中获取要用的产品
--2.在用的流向，否则认为该流向不可用，流向有新旧两张表
insert overwrite table dm_as.ts_supply_product_flow_price_m partition (inc_month = '$[month(yyyyMM,-1)]' )
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
      from (select price_product_code product_code
                   ,price_product_name product_name
              from dm_as.ts_supply_product_name_info a
              where is_valid=1
           ) a
      join (select * 
              from
                   (select product_name,product_code,source_code,source_name,dest_code,dest_name,pay_country,price_code,base_weight_qty,base_price,weight_price_qty,max_weight_qty,light_factor
                      from dm_as.joanna_price_base_all a
                      where inc_day ='$[day(yyyyMMdd,-1)]'   
                        and flow_type = '1'
                        and base_weight_qty>=0
                        and base_price>=0
                        and weight_price_qty>=0
                        and max_weight_qty>=0
                        and light_factor>=0
                        and product_code<>'SE0030'

                    union all 

                    select product_name,product_code,source_code,source_name,dest_code,dest_name,pay_country,price_code,base_weight_qty,base_price,weight_price_qty,max_weight_qty,light_factor 
                     from dm_as.ts_supply_product_cold_price_m where inc_month = '$[month(yyyyMM,-1)]'
                   ) t
            ) b on a.product_code=b.product_code
      join (--标识当前在用的流向
            select product_code,source_code,dest_code 
              from (select product_code,source_code,dest_code
                      from dm_pvs.tm_product_flow --新流向表
                      where inc_month = '$[day(yyyyMMdd,-1)]'
                        and invalid_date is null
                        and source_code is not null 
                        and dest_code is not null
                        and flow_type = '1'
                    union all
                    select product_code,source_code,dest_code
                      from ods_pvs.tm_price_flow   --老流向表
                      where inc_month = '$[month(yyyyMM,-1)]'
                        and invalid_date is null
                        and source_code is not null 
                        and dest_code is not null
                        and outside = '0'  
                    ) a
              group by product_code,source_code,dest_code
            ) c on b.product_code=c.product_code and b.source_code=c.source_code and b.dest_code=c.dest_code;
