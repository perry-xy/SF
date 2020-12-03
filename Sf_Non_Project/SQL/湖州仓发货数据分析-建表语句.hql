create table if not exists dm_as.cxy_wzps_hz_toC
(
    send_date           string   comment   '出库日期',
    create_time         string   comment   '创建时间',
    erp_no              string   comment    'erp运单',
    boci                string   comment    '波次号',
    waybill_no          string   comment    '运单号',
    order_type          string   comment    '出库类型',
    receive_wd          string   comment    '收货网点',
    receiver            string   comment    '收件人',
    address             string   comment    '地址',
    sku_no              string   comment    'sku号码',
    sku_name            string   comment    'sku名称',
    quantity            int      comment    '数量',
    single_fee          double   comment    '单价',
    total_fee           double   comment    '总计费重量'
)ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

load data inpath '/user/01397192/upload/part1.csv' into table dm_as.cxy_wzps_hz_toC;


create table tmp_dm_as.cxy_1126_tj as
select
  a.waybill_no,
  src_dist_code,
  dest_dist_code,
  receive_wd,
  receiver,
  send_date,
  boci,
  erp_no,
  product_code,
  real_weight_qty real_weight,
  meterage_weight_qty jifei_weight,
  all_fee_rmb fee
from
  (
    select
      waybill_no,
      max(receive_wd) receive_wd,
      max(receiver) receiver,
      max(boci) boci,
      max(erp_no) erp_no,
      max(send_date) send_date
    from
      dm_as.cxy_wzps_tj_toc
    group by
      waybill_no
  ) a
  left join (
    select
      waybill_no,
      src_dist_code,
      dest_dist_code,
      product_code,
      real_weight_qty,
      meterage_weight_qty,
      all_fee_rmb
    from
      gdl.tt_waybill_info
    where
      inc_day >= '20200815'
      and inc_day <= '20201126'
  ) b on a.waybill_no = b.waybill_no;


  select
  count(distinct waybill_no),
  percentile_approx(jifei_weight, array(0.05, 0.25, 0.5, 0.75, 0.95), 3000) weight_distribution,
  avg(jifei_weight) avg_weight,
  sum(fee) / sum(jifei_weight) avg_price,
  count(*) num
from
  tmp_dm_as.cxy_1126_dg
where
   jifei_weight is not null;
  
select
  src_dist_code,
  dest_dist_code,
  count(distinct waybill_no) num
from
  tmp_dm_as.cxy_1126_dg
where
  jifei_weight is not null
group by
  src_dist_code,
  dest_dist_code;