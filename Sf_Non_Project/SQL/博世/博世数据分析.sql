Alter table tmp_dm_as.cxy_bs_order_1701_2004 set serdeproperties ('serialization.encoding'='GBK')

drop table if exists tmp_dm_as.cxy_bs_date_1701_2004;
create table if not exists tmp_dm_as.cxy_bs_date_1701_2004 (
    date_ string,
    year_ STRING, 
    month STRING, 
    quarter string,
    year_iso STRING,
    week_iso string,
    w_year string,
    w_month string
) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;

load data inpath '/user/01397192/upload/date.csv' into table tmp_dm_as.cxy_bs_date_1701_2004;

--做成订单、时间、节假日的宽表
create table if not exists tmp_dm_as.cxy_bs_kuanbiao as
select warehouse_name,
 	create_date,
        item_type,
        category_1,
        category_2,
        quantity,
        year_,
        month,
        quarter,
        week,
        description
from
           (select warehouse_name,
                   create_date,
                   item_type,
                   category_1,
                   category_2,
                   sum(quantity) quantity
                   from 
                       --匹配skucag里的物料
                       (select a.warehouse_name,
                               a.create_date,
                               a.item,
                               b.item_type,
                               b.category_1,
                               b.category_2,
                               b.price,
                               b.fit_material,
                               b.source,
                               a.quantity,
                               a.province,
                               a.city,
                               a.inc_day
                               from tmp_dm_as.cxy_bs_order_1701_2004 a
                               join tmp_dm_as.cxy_bs_skucag b 
                               on a.item=b.item) aa
                               group by warehouse_name,
                                        create_date,
                                        item_type,
                                        category_1,
                                        category_2) aaa
        join   
            --匹配周数
            (select date_,
                    year_,
                    month,
                    quarter,
                    week_iso week
                    from tmp_dm_as.cxy_bs_date_1701_2004) bbb
        on aaa.create_date=bbb.date_
        left join 
            --匹配节假日
            (select date_,
                    description
                    from tmp_dm_as.cxy_bs_holidays) ccc
        on aaa.create_date=ccc.date_
union all
--不区分节假日时
select warehouse_name,
        item_type,
        category_1,
        category_2,
        year_,
        month,
        quarter,
        week,
        sum(quantity) total_quantity
        from tmp_dm_as.cxy_bs_kuanbiao
        group by warehouse_name,
                 item_type,
                 category_1,
                 category_2,
                 year_,
                 month,
                 quarter,
                 week;

--节假日
select  warehouse_name,
        item_type,
        category_1,
        category_2,
        year_,
        month_1 month,
        quarter_1 quarter,
        description week,
        sum(quantity) total_quantity
        from
                (select warehouse_name,
 	                create_date,
                        item_type,
                        category_1,
                        category_2,
                        quantity,
                        year_,
                        month,
                        quarter,
                        week week_1,
                        description,
                        '9999' month_1,
                        '0000' quarter_1
                        from tmp_dm_as.cxy_bs_kuanbiao) a
        group by warehouse_name,
                 item_type,
                 category_1,
                 category_2,
                 year_,
                 month_1,
                 quarter_1,
                 description;


--季节的周平均销量
select warehouse_name,
       item_type,
       month_,
       avg(quantity)
from
(select warehouse_name,
       item_type,
       month_,
       week_,
       sum(quantity) quantity
       from
(select  warehouse_name,
        create_date,
        item,
        quantity
        from tmp_dm_as.cxy_bs_order_1701_2004) a 
join   tmp_dm_as.cxy_bs_skucag b 
on     a.item=b.item
join  (select  date_,
                    year_,
                    month_,
                    week_iso week_
                    from tmp_dm_as.cxy_bs_date_1701_2004) c 
on a.create_date=c.date_
where warehouse_name='佛山仓' and item_type='AC'
group by warehouse_name,
       item_type,
       month_,
       week_) aa
group by warehouse_name,
       item_type,
       month_;


select
        warehouse_name,
        item_type,
        w_year,
        w_month,
        week_iso,
        sum(quantity)
from
        (
        select
                a.*,
                b.item_type,
                c.*
                from
                        tmp_dm_as.cxy_bs_order_1701_2004 a
                join tmp_dm_as.cxy_bs_skucag b 
                on a.item = b.item
                join tmp_dm_as.cxy_bs_date_1701_2004 c 
                on a.create_date = c.date_
                where
                        warehouse_name = '佛山仓'
                and item_type = 'BE'
                                                ) aa
group by
        warehouse_name,
        item_type,
        w_year,
        w_month,
        week_iso;


--通用求周度、季节的周平均，周度蕴含其中
select warehouse_name,
		item_type,
        year_,
        season,
        avg(quantity) 
from 
(select aaaa.*,
		case when month_='4' then '春天' 
        when (month_='11' or month_='12' or month_='1' or month_='2' or month_='3') then '冬天' 
        when month_='10' then '秋天' 
        else '夏天' end season
from 
(select warehouse_name,
        item_type,
        year_,
        month_,
        week_,
        sum(quantity) quantity
from 
(select case when aa.warehouse_name is null then '福州仓'
        else aa.warehouse_name end warehouse_name,
        case when aa.item_type is null then 'AC'
        else aa.item_type end item_type,
        case when aa.quantity is null then 0
        else aa.quantity end quantity,
        bb.w_year year_,
        bb.w_month month_,
        bb.week_iso week_,
        bb.description holiday
from
        (select a.warehouse_name,
                a.item,
                a.create_date,
                b.item_type,
                sum(a.quantity) quantity
                from tmp_dm_as.cxy_bs_order_1701_2004 a 
                join tmp_dm_as.cxy_bs_skucag b 
                on a.item=b.item
                where a.warehouse_name='福州仓' 
                and b.item_type='AC'
                group by a.warehouse_name,
                        a.item,
                        a.create_date,
                        b.item_type   )aa 
right join  (select * from tmp_dm_as.cxy_bs_date where year_!='2016') bb 
on aa.create_date=bb.date_) aaa
group by  warehouse_name,
        item_type,
        year_,
        month_,
        week_) aaaa ) aaaaa
group by warehouse_name,
		item_type,
        year_,
        season;


--节假日
select year_,
       holiday,
       month_,
       week_,
       quantity
from
(select warehouse_name,
       item_type,
       year_,
       month_,
       week_,
       holiday,
       sum(quantity) quantity
from 
(select case when aa.warehouse_name is null then '福州仓'
        else aa.warehouse_name end warehouse_name,
        case when aa.item_type is null then 'AC'
        else aa.item_type end item_type,
        case when aa.quantity is null then 0
        else aa.quantity end quantity,
        bb.w_year year_,
        bb.w_month month_,
        bb.week_iso week_,
        bb.description holiday
from
        (select a.warehouse_name,
                a.item,
                a.create_date,
                b.item_type,
                sum(a.quantity) quantity
                from tmp_dm_as.cxy_bs_order_1701_2004 a 
                join tmp_dm_as.cxy_bs_skucag b 
                on a.item=b.item
                where a.warehouse_name='福州仓' 
                and b.item_type='AC'
                group by a.warehouse_name,
                        a.item,
                        a.create_date,
                        b.item_type   )aa 
right join  (select * from tmp_dm_as.cxy_bs_date where year_!='2016') bb 
on aa.create_date=bb.date_) aaa
group by warehouse_name,
       item_type,
       year_,
       month_,
       week_,
       holiday) aaaa
where holiday!='workingday';


--18年有而17年没有的客户
select 
w_year,
new_ship_to,
item,
sum(quantity) 
from tmp_dm_as.cxy_bs_order a 
join tmp_dm_as.cxy_bs_skucag b 
on a.item=b.item 
join tmp_dm_as.cxy_bs_date c 
on a.create_date=c.date_
where warehouse_name='佛山仓' and item_type='AC' 
group by w_year,
new_ship_to,
item;

to_date(
    from_unixtime(UNIX_TIMESTAMP(order_date, 'yyyy/MM/dd'))
  )




