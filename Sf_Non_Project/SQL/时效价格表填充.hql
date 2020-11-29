select 
    a.src_code,
    a.dest_code,
    a.distance,
    nvl(a.transit_price,nvl(b.transit_price,nvl(c.transit_price,d.transit_price))) transit_price,
    nvl(a.trunk_price,nvl(b.trunk_price,nvl(c.trunk_price,d.trunk_price))) trunk_price,
    case when a.transit_price is not null then 0
		 when a.transit_price is null and b.transit_price is not null then 1
        when a.transit_price is null and b.transit_price is  null and c.transit_price is not null then 2
        when a.transit_price is null and b.transit_price is  null and c.transit_price is  null and d.transit_price is not null then 3 
        else 4 end transit_add_mark,
    case when a.trunk_price is not null then 0
		 when a.trunk_price is null and b.trunk_price is not null then 1
        when a.trunk_price is null and b.trunk_price is  null and c.trunk_price is not null then 2
        when a.trunk_price is null and b.trunk_price is  null and c.trunk_price is  null and d.trunk_price is not null then 3 
        else 4 end trunk_add_mark
from 
    tmp_dm_as.cxy_transit_trunk a
left join
    (select src_province,
		    dest_province,
		    avg(transit_price) transit_price,
		    avg(trunk_price) trunk_price
		    from tmp_dm_as.cxy_transit_trunk 
		    group by src_province,
                    dest_province) b
    on a.src_province=b.src_province and a.dest_province = b.dest_province
left join
    (select src_area,
		    dest_area,
		    avg(transit_price) transit_price,
		    avg(trunk_price) trunk_price
		    from tmp_dm_as.cxy_transit_trunk 
		    group by src_area,
                    dest_area) c
    on a.src_area=c.src_area and a.dest_area=c.dest_area
left join
    (select distance_section,
		    avg(transit_price) transit_price,
		    avg(trunk_price) trunk_price
		    from tmp_dm_as.cxy_transit_trunk 
		    group by distance_section) d
    on a.distance_section = d.distance_section;