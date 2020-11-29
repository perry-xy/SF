--总量统计
select date_,
       count(distinct erp_order_no) total_num,
       count(distinct case when gap <0 then erp_order_no end) odnormal,
       count(distinct case when gap <=24 and gap >=0 then erp_order_no end) finished,
       count(distinct case when gap >24 then erp_order_no end) unfinished
from
(select erp_order_no,
       BAR_SCAN_DATETIME,
       trans_BAR_SCAN_DATETIME,
       (unix_timestamp(trans_BAR_SCAN_DATETIME) - unix_timestamp(BAR_SCAN_DATETIME)) /3600 gap,
       date_
    from
    (select
        aa.erp_order_no,
        aa.BAR_SCAN_DATETIME,
        case when bb.BAR_SCAN_DATETIME is not null then bb.BAR_SCAN_DATETIME 
                    else '2099-01-01 00:00:00' end trans_BAR_SCAN_DATETIME,
        substr(aa.BAR_SCAN_DATETIME,1,10) date_
        from
        (select *
                    from 
                        (select  erp_order_no, 
                                 BAR_SCAN_DATETIME, 
                                 row_number() over(partition by erp_order_no order by BAR_SCAN_DATETIME asc) rank
                                 from dwd_tsfc.dwd_scmw_estee_cas_order_activity_history_di
                            where 
                                    BAR_SCAN_DATETIME>='2020-11-01 00:00:00' 
                                and BAR_SCAN_DATETIME<='2020-11-08 00:00:00'
                                and domain= 'TMS_PICKED' --揽收路由
                                and inc_day >= '20201101' 
                                and inc_day <= '20201109'
											) a

        where rank = 1) aa
left join 
        (select * 
                    from 
                        (select erp_order_no,
                                BAR_SCAN_DATETIME,
                                row_number() over(partition by erp_order_no order by BAR_SCAN_DATETIME asc) rank
                                 from dwd_tsfc.dwd_scmw_estee_cas_order_activity_history_di
                            where  
                                    BAR_SCAN_DATETIME >= '2020-11-01 00:00:00' 
                                and BAR_SCAN_DATETIME < '2020-11-09 00:00:00'
                                and domain= 'TRANSPORTING' -- 运输路由
                                and inc_day >= '20201101' 
                                and inc_day <= '20201109'
									) a
        where rank = 1) bb
on aa.erp_order_no = bb.erp_order_no)  aaa
) aaaa
group by date_;

--天猫统计
select date_,
       count(distinct aaaa.erp_order_no) total_num,
       count(distinct case when gap <0 then aaaa.erp_order_no end) odnormal,
       count(distinct case when gap <=24 and gap >=0 then aaaa.erp_order_no end) finished,
       count(distinct case when gap >24 then aaaa.erp_order_no end) unfinished
from
(select erp_order_no,
       BAR_SCAN_DATETIME,
       trans_BAR_SCAN_DATETIME,
       (unix_timestamp(trans_BAR_SCAN_DATETIME) - unix_timestamp(BAR_SCAN_DATETIME)) /3600 gap,
       date_
    from
    (select
        aa.erp_order_no,
        aa.BAR_SCAN_DATETIME,
        case when bb.BAR_SCAN_DATETIME is not null then bb.BAR_SCAN_DATETIME 
                    else '2099-01-01 00:00:00' end trans_BAR_SCAN_DATETIME,
        substr(aa.BAR_SCAN_DATETIME,1,10) date_
        from
        (select *
                    from 
                        (select  erp_order_no, 
                                 BAR_SCAN_DATETIME, 
                                 row_number() over(partition by erp_order_no order by BAR_SCAN_DATETIME asc) rank
                                 from dwd_tsfc.dwd_scmw_estee_cas_order_activity_history_di
                            where 
                                    BAR_SCAN_DATETIME>='2020-11-01 00:00:00' 
                                and BAR_SCAN_DATETIME<='2020-11-08 00:00:00'
                                and domain= 'TMS_PICKED' --揽收路由
                                and inc_day >= '20201101' 
                                and inc_day <= '20201109'
											) a

        where rank = 1) aa
left join 
        (select * 
                    from 
                        (select erp_order_no,
                                BAR_SCAN_DATETIME,
                                row_number() over(partition by erp_order_no order by BAR_SCAN_DATETIME asc) rank
                                 from dwd_tsfc.dwd_scmw_estee_cas_order_activity_history_di
                            where  
                                    BAR_SCAN_DATETIME >= '2020-11-01 00:00:00' 
                                and BAR_SCAN_DATETIME < '2020-11-09 00:00:00'
                                and domain= 'TRANSPORTING' -- 运输路由
                                and inc_day >= '20201101' 
                                and inc_day <= '20201109'
									) a
        where rank = 1) bb
on aa.erp_order_no = bb.erp_order_no)  aaa
) aaaa
join 
(select erp_order_no,
       user_def19 
       from 
       dwd_tsfc.dwd_scmw_estee_cas_sale_order_di
       where user_def19 like '%天猫%' 
       and inc_day >='20201001') bbbb
on aaaa.erp_order_no = bbbb.erp_order_no
group by date_;

--拆分品牌与供应维度
select distinct a.date_,
	   b.carrier,
       c.user_def19,
       c.package_note,
       c.sf_order_type,
       a.erp_order_no
       from
(select erp_order_no,
	   bar_scan_datetime,
       trans_bar_scan_datetime,
       gap,
       date_
	   from tmp_dm_as.cxy_ysld_tmp3
where gap >24) a 
left join 
 (select erp_order_no,
  		 	carrier
         from  dwd_tsfc.dwd_scmw_estee_cas_order_carrier_di
  		 where inc_day >= '202010101'
				and cas_order_type = 'SALE_ORDER'
						) b
on a.erp_order_no = b.erp_order_no
left join 
(select erp_order_no,
       package_note,
       sf_order_type,
       user_def19 
       from 
       dwd_tsfc.dwd_scmw_estee_cas_sale_order_di
       where 
         inc_day >='20201001') c
on a.erp_order_no = c.erp_order_no;

--各种品牌———总量统计（表已换）
select date_,
        data_from_db,
       count(distinct erp_order_no) total_num,
       count(distinct case when gap <0 then erp_order_no end) odnormal,
       count(distinct case when gap <=24 and gap >=0 then erp_order_no end) finished,
       count(distinct case when gap >24 then erp_order_no end) unfinished
from
(select erp_order_no,
        data_from_db,
       BAR_SCAN_DATETIME,
       trans_BAR_SCAN_DATETIME,
       (unix_timestamp(trans_BAR_SCAN_DATETIME) - unix_timestamp(BAR_SCAN_DATETIME)) /3600 gap,
       date_
    from
    (select
        aa.erp_order_no,
        aa.BAR_SCAN_DATETIME,
        aa.data_from_db,
        case when bb.BAR_SCAN_DATETIME is not null then bb.BAR_SCAN_DATETIME 
                    else '2099-01-01 00:00:00' end trans_BAR_SCAN_DATETIME,
        substr(aa.BAR_SCAN_DATETIME,1,10) date_
        from
        (select *
                    from 
                        (select  erp_order_no, 
                                 BAR_SCAN_DATETIME, 
                                 data_from_db,
                                 row_number() over(partition by erp_order_no order by BAR_SCAN_DATETIME asc) rank
                                 from dws_tsfc.dws_inte_scct_all_cas_order_activity_history_di
                            where 
                                    ((BAR_SCAN_DATETIME >= '2020-11-01 00:00:00' 
                                and BAR_SCAN_DATETIME < '2020-11-06 00:00:00') or 
                                (BAR_SCAN_DATETIME >= '2020-11-11 00:00:00' 
                                and BAR_SCAN_DATETIME < '2020-11-13 00:00:00'))
                                and domain= 'TMS_PICKED' --揽收路由
                                and inc_day >= '20201101' 
                                and inc_day <= '20201115'
											) a

        where rank = 1) aa
left join 
        (select * 
                    from 
                        (select erp_order_no,
                                BAR_SCAN_DATETIME,
                                data_from_db,
                                row_number() over(partition by erp_order_no order by BAR_SCAN_DATETIME asc) rank
                                 from dws_tsfc.dws_inte_scct_all_cas_order_activity_history_di
                            where  
                                ((BAR_SCAN_DATETIME >= '2020-11-01 00:00:00' 
                                and BAR_SCAN_DATETIME < '2020-11-07 00:00:00') or 
                                (BAR_SCAN_DATETIME >= '2020-11-11 00:00:00' 
                                and BAR_SCAN_DATETIME < '2020-11-14 00:00:00'))
                                and domain= 'TRANSPORTING' -- 运输路由
                                and inc_day >= '20201101' 
                                and inc_day <= '20201115'
									) a
        where rank = 1) bb
on aa.erp_order_no = bb.erp_order_no)  aaa
) aaaa
group by date_,
data_from_db;

--付款&有运输路由&无揽收路由的订单
select
  a.erp_order_no,
  a.package_note,
  a.sf_order_type,
  a.user_def19,
  b.BAR_SCAN_DATETIME
from
  (
    select
      erp_order_no,
      package_note,
      sf_order_type,
      user_def19,
      status,
      pay_datetime
    from
      dwd_tsfc.dwd_scmw_estee_cas_sale_order_di
    where
      inc_day >= '20201101'
      and pay_datetime >= '2020-11-01 00:00:00'
      and pay_datetime < '2020-11-12 00:00:00'
      and status > 3900
      and status != 9999
  ) a
  left join (
    select
      erp_order_no,
      BAR_SCAN_DATETIME
    from
      dwd_tsfc.dwd_scmw_estee_cas_order_activity_history_di
    where
      DOMAIN = 'TMS_PICKED'
      and create_datetime >= '2020-11-01 00:00:00'
      and inc_day >= '20201101'
  ) b on a.erp_order_no = b.erp_order_no
where
  b.BAR_SCAN_DATETIME is null;