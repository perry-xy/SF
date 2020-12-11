import pandas as pd
import gc
from pyecharts import options as opts
from pyecharts.charts import Geo
from pyecharts.globals import ChartType, SymbolType
#from pyecharts.faker import Faker
from pyecharts.globals import GeoType
import datetime
import xlwings as xw
import os

class Config():
    """
    模型调节参数，可用来调节是否考虑调拨成本、支线的配送方式是to_B或to_C等
    """
    reverse_compute = True
    distribute_ToB = False
    distribute_ToC = True

    avg_weight = 12.5
    year_day = 365

class FacilitylocationCurrentstatus():
    """
    仓网规划现状计算
    """
    def __init__(self,filename,config):
        """
        整理信息
        """
        self.filename=filename
        self.config=config
        self.weight_avg = config.avg_weight
        self.YEAR_DAY = config.year_day
        self.sku=dict()
        self.demand=dict()
        self.warehouse=dict()
        self.distribution_price=dict()
        self.trunk_price=dict()
        self.cdc_category_capacity=dict()
        self.GIS=dict()
        self.warehouse_area_ratio=dict()
        self.factory_total=list()
        self.category_list=list()
        self.rdc=list()
        self.customer=list()
        self.cdc_cand=list()
        self.reverse_customer=list()
        self.reverse_tob=dict()
        self.reverse_price=dict()
        self.dis_price_toB_trunk = dict()
        self.dis_price_toB_transit=dict()
        self.dis_price_toB_dist=dict()
        self.datahandle()

    def datahandle(self):
        """
                    类型转换；编写字典
                                        """
        print("开始读取数据："+str(datetime.datetime.now()))
        app = xw.App(visible=False, add_book=False)
        wb = app.books.open(self.filename)

        demand_df = wb.sheets["Demand_toC"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        sku_df = wb.sheets["SKU_Info"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                dtype=object).value
        distribution_price_df = wb.sheets["Distribution"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                dtype=object).value
        warehouse_df = wb.sheets["Warehouse"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                dtype=object).value
        trunk_price_df = wb.sheets["Trunk"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                dtype=object).value
        factory_capacity_df = wb.sheets["Factory"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                dtype=object).value
        gis = wb.sheets["GIS"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                dtype=object).value
        factory_total_df = wb.sheets["Factory_Total"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                dtype=object).value
        warehouse_area_ratio = wb.sheets["仓变系数"].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                                dtype=object).value
        reverse_tob_df = wb.sheets['Reverse_toB'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        # reverse_price_df = wb.sheets['Reverse_Price'].range('A1').options(pd.DataFrame, expand='table', index=False,
        #                                                        dtype=object).value
        reverse_price_df = pd.read_csv("data/Reverse_Price.csv")
        # dis_price_toB_trunk_df = wb.sheets['Dis_Trunk'].range('A1').options(pd.DataFrame, expand='table',index=False,
        #                                                        dtype=object).value
        dis_price_toB_trunk_df = pd.read_csv("data/Dis_Trunk.csv")
        dis_price_toB_transit_df = wb.sheets['Dis_Transit'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        dis_price_toB_dist_df = wb.sheets['Dis_Dist'].range('A1').options(pd.DataFrame, expand='table', index=False,
                                                               dtype=object).value
        wb.close()
        app.quit()

        demand_df[['起始城市代码','目的城市代码']] = demand_df[['起始城市代码','目的城市代码']].astype(int).astype(str)
        distribution_price_df[['src_code', 'dest_code']] = distribution_price_df[['src_code', 'dest_code']].astype(
            int).astype(str)
        warehouse_df['城市代码'] = warehouse_df['城市代码'].astype(int).astype(str)
        trunk_price_df[['src_code', 'dest_code']] = trunk_price_df[['src_code', 'dest_code']].astype(int).astype(str)
        factory_capacity_df[['CDC_Name','RDC_Name']] = factory_capacity_df[['CDC_Name','RDC_Name']].astype(int).astype(str)
        gis['城市代码'] = gis['城市代码'].astype(int).astype(str)
        factory_total_df['城市代码'] = factory_total_df['城市代码'].astype(int).astype(str)
        #warehouse_area_ratio['仓数'] = warehouse_area_ratio['仓数'].astype(int)
        reverse_tob_df[['起始城市代码','目的城市代码','车型']] = reverse_tob_df[['起始城市代码','目的城市代码','车型']].astype(int).astype(str)
        reverse_price_df[['src_code', 'dest_code', '车型']] = reverse_price_df[['src_code', 'dest_code', '车型']].astype(int).astype(str)
        print(reverse_price_df.head())
        dis_price_toB_trunk_df[['src_code', 'dest_code']] = dis_price_toB_trunk_df[['src_code', 'dest_code']].astype(int).astype(str)
        dis_price_toB_transit_df[['src_code', 'dest_code']] = dis_price_toB_transit_df[['src_code', 'dest_code']].astype(int).astype(str)
        dis_price_toB_dist_df['始发城市'] = dis_price_toB_dist_df['始发城市'].astype(int).astype(str)

        # sku、category_list
        for idx, row in sku_df.iterrows():
            self.category_list.append(row['SKU代码'])
            self.sku[row['SKU代码'].lower()] = {'name': row['SKU名称'],
                                  'turnoverdays': row['周转天数'],
                                  'area_weight_ratio': row['面积重量比（m^2/kg)']}
        self.category_list.sort()

        # demand、customer
        for idx, row in demand_df.iterrows():
            self.rdc.append(row['起始城市代码'])
            self.customer.append(row['目的城市代码'])
            self.demand[(row['起始城市代码'], row['目的城市代码'])] = dict()
            for sku in self.category_list:
                self.demand[(row['起始城市代码'],row['目的城市代码'])][sku.lower()] = row[sku]
            self.demand[(row['起始城市代码'], row['目的城市代码'])]['weight'] = row['总重量']
        self.rdc=list(set(self.rdc))

        # warehouse
        for idx, row in warehouse_df.iterrows():
            self.warehouse[row['城市代码']] = {'rental': row['租金（元/月/平方米)'],
                                           'upper_area': row['面积上限'],
                                           'ware_in_fee': row['入库操作费用（元/公斤）'],
                                           'ware_out_fee': row['出库操作费用（元/公斤）']}

        # distribution_price
        for idx, row in distribution_price_df.iterrows():
            self.distribution_price[(row['src_code'], row['dest_code'])] = {'distance': row['distance'],
                                                                            'base_qty': row['首重'],
                                                                            'base_price': row['首重价格'],
                                                                            'weight_price_qty': row['续重价格']}

        # trunk_price
        for idx, row in trunk_price_df.iterrows():
            self.trunk_price[(row['src_code'], row['dest_code'])] = {'trans_fee': row['运输价格（元/公斤）'],
                                                                     'distance': row['distance']}
        # factory_capacity、cdc_cand
        for idx, row in factory_capacity_df.iterrows():
            self.cdc_cand.append(row['CDC_Name'])
            self.cdc_category_capacity[(row['CDC_Name'], row['RDC_Name'])]=dict()
            for sku in self.category_list:
                self.cdc_category_capacity[(row['CDC_Name'],row['RDC_Name'])][sku.lower()] = row[sku]
        self.cdc_cand = list(set(self.cdc_cand))

        #gis
        for idx, row in gis.iterrows():
            self.GIS[row['城市代码']] = {'city_name_cn': row['城市名'],
                                     'city_name_en': row['city_name'],
                                     'lng': row['lng'],
                                     'lat': row['lat'],
                                     'province': row['省']}

        #total_factory
        for idx,row in factory_total_df.iterrows():
            self.factory_total.append(row['城市代码'])

        # warehouse_area_ratio
        for idx, row in warehouse_area_ratio.iterrows():
            self.warehouse_area_ratio[row['仓数']] = row['系数']

        # reverse_tob
        for idx, row in reverse_tob_df.iterrows():
            self.reverse_customer.append(row['目的城市代码'])
            self.reverse_tob[row['起始城市代码'],row['目的城市代码']] = dict()
            for sku in self.category_list:
                self.reverse_tob[row['起始城市代码'],row['目的城市代码']][sku.lower()] = row[sku.upper()]
            self.reverse_tob[row['起始城市代码'],row['目的城市代码']]['weight'] = row['WEIGHT']
            self.reverse_tob[row['起始城市代码'],row['目的城市代码']]['vehicle'] = row['车型']

        # reverse_price
        for idx, row in reverse_price_df.iterrows():
            src = row['src_code']
            dest = row['dest_code']
            vehicle = row['车型']
            self.reverse_price[(src, dest, vehicle)] = {'distance': row['distance'],
                                                            'trans_fee': row['price/kg']}

        # dis_price_toB_trunk
        for idx, row in dis_price_toB_trunk_df.iterrows():
                src = row['src_code']
                dest = row['dest_code']
                self.dis_price_toB_trunk[(src, dest)] = {'distance': row['distance'],
                                                            'trans_fee': row['price/kg']}

        # dis_price_toB_transit
        for idx, row in dis_price_toB_transit_df.iterrows():
            src = row['src_code']
            dest = row['dest_code']
            self.dis_price_toB_transit[(src, dest)] = {'transit_fee': row['price']}

        # dis_price_toB_dist
        for idx, row in dis_price_toB_dist_df.iterrows():
            self.dis_price_toB_dist[row['始发城市']] = {'dist_fee': row['元/kg']}

        del sku_df
        del demand_df
        del warehouse_df
        del distribution_price_df
        del trunk_price_df
        del factory_capacity_df
        del gis
        del factory_total_df
        del warehouse_area_ratio
        del reverse_tob_df
        del reverse_price_df
        del dis_price_toB_trunk_df
        del dis_price_toB_transit_df
        del dis_price_toB_dist_df
        gc.collect()

        print("数据处理完毕："+str(datetime.datetime.now()))

    def rdc_shipping_cost_to_c(self):
        """
        支线运输成本——toC模式
        """
        shipping_cost_c=0
        shipping_distance_c=0
        shipping_cost_d=dict()
        shipping_distance_d = dict()
        rdc_count_d=dict()
        rdc_weight_d=dict()

        for rdc in self.rdc:
            rdc_cost=0
            rdc_distance=0
            rdc_customer=0
            rdc_weight=0
            for key,value in self.demand.items():
                if key[0]==rdc:
                    rdc_cost += ((self.weight_avg - self.distribution_price[key]['base_qty']) * \
                            self.distribution_price[key]['weight_price_qty'] + self.distribution_price[key]['base_price']) * \
                                value['weight']/self.weight_avg
                    rdc_distance += self.distribution_price[key]['distance']
                    rdc_customer +=1
                    rdc_weight += value['weight']
            shipping_cost_c += rdc_cost
            shipping_distance_c += rdc_distance
            shipping_cost_d[rdc]=rdc_cost
            shipping_distance_d[rdc] = rdc_distance
            rdc_count_d[rdc] = rdc_customer
            rdc_weight_d[rdc] = rdc_weight

        return shipping_cost_c, shipping_cost_d, shipping_distance_c, shipping_distance_d, rdc_count_d, rdc_weight_d

    def rdc_shipping_cost_to_b(self):
        """
        支线运输成本——toB模式
        """
        # 仓 → 目的地中转场 → 中转 → 支线
        # 参数
        trunk_price = self.dis_price_toB_trunk
        # trunk_price = self.trunk_price
        transit_price = self.dis_price_toB_transit
        dist_price = self.dis_price_toB_dist

        # 计算变量
        shipping_cost_d = dict()  # 各rdc的配送花费
        shipping_cost_trunk = 0  # 支线干线配送成本
        shipping_cost_transit = 0  # 支线中转配送成本
        shipping_cost_dist = 0  # 支线支线配送成本
        shipping_cost = 0  # 支线配送总成本
        shipping_distance_d = dict()  # 各rdc的配送距离
        shipping_distance = 0  # 总配送距离
        rdc_count_d = dict()
        rdc_weight_d = dict()

        for rdc_name in self.rdc:
            trunk = 0
            transit = 0
            dist = 0
            cost = 0
            distance = 0
            rdc_customer = 0
            rdc_weight = 0
            for key, value in self.demand.items():
                rdc = key[0]
                customer = key[1]
                if key[0] == rdc_name:
                    trunk += value['weight'] * trunk_price[key]['trans_fee']
                    transit += value['weight'] * transit_price[customer,customer]['transit_fee']
                    dist +=  value['weight'] * dist_price[customer]['dist_fee']
                    distance +=  trunk_price[key]['distance']
                    rdc_customer += 1
                    rdc_weight += value['weight']
            cost = trunk + transit + dist
            shipping_distance += distance
            shipping_cost_trunk += trunk  # 支线干线配送成本
            shipping_cost_transit += transit  # 支线中转配送成本
            shipping_cost_dist += dist  # 支线支线配送成本
            shipping_cost += cost
            shipping_cost_d[rdc_name] = dict()
            shipping_cost_d[rdc_name]['trunk'] = trunk
            shipping_cost_d[rdc_name]['transit'] = transit
            shipping_cost_d[rdc_name]['dist'] = dist
            shipping_cost_d[rdc_name]['cost'] = cost
            rdc_weight_d[rdc_name] = rdc_weight
            rdc_count_d[rdc_name] = rdc_customer
            shipping_distance_d[rdc_name] = distance

        return shipping_cost, shipping_cost_trunk, shipping_cost_transit, shipping_cost_dist, shipping_cost_d, \
               shipping_distance, shipping_distance_d, rdc_count_d, rdc_weight_d


    def cdc_shipping_cost(self):
        """
        干线运输成本
        """
        shipping_cost_c=0
        shipping_cost_d=dict()
        for rdc in self.rdc:
            rdc_shipping_cost=0
            for key,value in self.cdc_category_capacity.items():
                if key[1] == rdc:
                    for sku in self.category_list:
                        rdc_shipping_cost += value[sku.lower()] * self.trunk_price[key]['trans_fee']
            shipping_cost_c += rdc_shipping_cost
            shipping_cost_d[rdc] = rdc_shipping_cost

        return shipping_cost_c, shipping_cost_d


    def rdc_storage_cost(self):
        """
        仓库租金、操作费用
        """
        demand_df = pd.DataFrame(self.demand).T
        demand_df.index = [z[0] for z in demand_df.index]
        rdc_demand_df = demand_df.groupby([demand_df.index])[[ x.lower() for x in self.category_list]+['weight']].\
                            agg([('weight','sum')])
        rdc_demand = {}
        for idx, row in rdc_demand_df.iterrows():
            rdc_demand[idx] = dict()
            for sku in self.category_list:
                rdc_demand[idx][sku.lower()] = row[sku.lower()]['weight']
            rdc_demand[idx]['weight'] = row['weight']['weight']

        rental_cost = 0
        area = 0
        in_handling_cost = 0
        out_handling_cost = 0
        rental_cost_d = dict()
        area_d = dict()
        in_handling_cost_d = dict()
        out_handling_cost_d = dict()

        for rdc in self.rdc:
            rdc_area = 0
            rdc_rental = 0
            rdc_in_handling = 0
            rdc_out_handling = 0
            for sku in self.category_list:
                rdc_area += rdc_demand[rdc][sku.lower()] / self.YEAR_DAY  * self.sku[sku.lower()]['turnoverdays'] * \
                            self.sku[sku.lower()]['area_weight_ratio'] * self.warehouse_area_ratio[len(self.rdc)]
            rdc_rental = rdc_area * self.warehouse[rdc]['rental'] * 12
            rdc_in_handling = rdc_demand[rdc]['weight'] * self.warehouse[rdc]['ware_in_fee']
            rdc_out_handling = rdc_demand[rdc]['weight'] * self.warehouse[rdc]['ware_out_fee']
            area += rdc_area
            rental_cost += rdc_rental
            in_handling_cost += rdc_in_handling
            out_handling_cost += rdc_out_handling
            rental_cost_d[rdc] = rdc_rental
            area_d[rdc] = rdc_area
            in_handling_cost_d[rdc] = rdc_in_handling
            out_handling_cost_d[rdc] = rdc_out_handling

        if self.config.reverse_compute is True:
            reverse_df = pd.DataFrame(self.reverse_tob).T
            reverse_df.index = [z[0] for z in reverse_df.index]
            rdc_reverse_df = reverse_df.groupby([reverse_df.index])[[ x.lower() for x in self.category_list]+['weight']].\
                                agg([('weight','sum')])
            reverse_demand = {}
            for idx, row in rdc_reverse_df.iterrows():
                reverse_demand[idx] = dict()
                for sku in self.category_list:
                    reverse_demand[idx][sku.lower()] = row[sku.lower()]['weight']
                reverse_demand[idx]['weight'] = row['weight']['weight']

            reverse_rental_cost = 0
            reverse_area = 0
            reverse_in_handling_cost = 0
            reverse_out_handling_cost = 0
            reverse_rental_cost_d = dict()
            reverse_area_d = dict()
            reverse_in_handling_cost_d = dict()
            reverse_out_handling_cost_d = dict()

            for rdc in self.rdc:
                re_rdc_area = 0
                re_rdc_rental = 0
                re_rdc_in_handling = 0
                re_rdc_out_handling = 0
                for sku in self.category_list:
                    re_rdc_area += reverse_demand[rdc][sku.lower()] / self.YEAR_DAY * self.sku[sku.lower()]['turnoverdays'] * \
                                self.sku[sku.lower()]['area_weight_ratio'] * self.warehouse_area_ratio[len(self.rdc)]
                re_rdc_rental = re_rdc_area * self.warehouse[rdc]['rental'] * 12
                re_rdc_in_handling = reverse_demand[rdc]['weight'] * self.warehouse[rdc]['ware_in_fee']
                re_rdc_out_handling = reverse_demand[rdc]['weight'] * self.warehouse[rdc]['ware_out_fee']
                reverse_area += re_rdc_area
                reverse_rental_cost += re_rdc_rental
                reverse_in_handling_cost += re_rdc_in_handling
                reverse_out_handling_cost += re_rdc_out_handling
                reverse_rental_cost_d[rdc] = re_rdc_rental
                reverse_area_d[rdc] = re_rdc_area
                reverse_in_handling_cost_d[rdc] = re_rdc_in_handling
                reverse_out_handling_cost_d[rdc] = re_rdc_out_handling

        if self.config.reverse_compute is False:
            return rental_cost , rental_cost_d, area, area_d, in_handling_cost, in_handling_cost_d, \
                   out_handling_cost, out_handling_cost_d
        else:
            return rental_cost, rental_cost_d, area, area_d, in_handling_cost, in_handling_cost_d, \
                   out_handling_cost, out_handling_cost_d, reverse_rental_cost, reverse_rental_cost_d, \
                   reverse_area, reverse_area_d, reverse_in_handling_cost, reverse_in_handling_cost_d, \
                   reverse_out_handling_cost, reverse_out_handling_cost_d

    def reverse_cost(self):
        """
        调拨成本计算
        """
        # 参数
        rdc_cand = self.rdc
        reverse_customer = self.reverse_customer
        reverse_tob = self.reverse_tob
        reverse_price = self.reverse_price

        # 计算变量
        reverse_cost = 0
        reverse_cost_d = dict()
        reverse_distance = 0
        reverse_distance_d = dict()
        reverse_weight_d = dict()
        reverse_count_d = dict()

        for rdc_name in rdc_cand:
            rdc_reverse_cost = 0
            rdc_reverse_distance = 0
            rdc_weight = 0
            rdc_count = 0
            for key,value in reverse_tob.items():
                if rdc_name == key[0]:
                    vehicle = value['vehicle']
                    rdc,name = key
                    rdc_reverse_cost += value['weight'] * reverse_price[rdc,name,vehicle]['trans_fee']
                    rdc_reverse_distance +=  reverse_price[rdc,name,vehicle]['distance']
                    rdc_weight += value['weight']
                    rdc_count += 1
            reverse_cost += rdc_reverse_cost
            reverse_distance += rdc_reverse_distance
            reverse_cost_d[rdc_name] = rdc_reverse_cost
            reverse_distance_d[rdc_name] = rdc_reverse_distance
            reverse_weight_d[rdc_name] = rdc_weight
            reverse_count_d[rdc_name] = rdc_count

        return reverse_cost, reverse_distance, reverse_cost_d, reverse_distance_d, reverse_count_d, reverse_weight_d

    def resultformat(self):
        """
        结果输出为两个文件：一个总performance,一个RDC的属性
        """
        if self.config.distribute_ToC is True:
            rdc_shipping_cost, rdc_shipping_cost_d, rdc_shipping_distance, rdc_shipping_distance_d, \
                rdc_count_d, rdc_weight_d = self.rdc_shipping_cost_to_c()
        else:
            rdc_shipping_cost, shipping_cost_trunk, shipping_cost_transit, shipping_cost_dist, rdc_shipping_cost_d, \
            rdc_shipping_distance, rdc_shipping_distance_d, rdc_count_d, rdc_weight_d = self.rdc_shipping_cost_to_b()

        cdc_shipping_cost, cdc_shipping_cost_d = self.cdc_shipping_cost()

        if self.config.reverse_compute is False:
            rental_cost, rental_cost_d, area, area_d, in_handling_cost, in_handling_cost_d, out_handling_cost, out_handling_cost_d\
                    = self.rdc_storage_cost()
        else:
            rental_cost, rental_cost_d, area, area_d, in_handling_cost, in_handling_cost_d, out_handling_cost, out_handling_cost_d,\
            reverse_rental_cost, reverse_rental_cost_d, reverse_area, reverse_area_d, reverse_in_handling_cost, reverse_in_handling_cost_d, \
            reverse_out_handling_cost, reverse_out_handling_cost_d = self.rdc_storage_cost()

        if self.config.reverse_compute is True:
            reverse_cost, reverse_distance, reverse_cost_d, reverse_distance_d,reverse_count_d, reverse_weight_d = \
                                                                                                self.reverse_cost()
        performance = {}
        performance['trans_cost'] = cdc_shipping_cost
        performance['dist_cost'] = rdc_shipping_cost
        performance['storage_cost'] = rental_cost
        performance['in_bound_cost'] = in_handling_cost
        performance['out_bound_cost'] = out_handling_cost
        if self.config.reverse_compute is True:
            performance['reverse_cost'] = reverse_cost
            performance['reverse_storage_cost'] = reverse_rental_cost
            performance['reverse_in_bound_cost'] = reverse_in_handling_cost
            performance['reverse_out_bound_cost'] = reverse_out_handling_cost
            performance['total_cost'] = cdc_shipping_cost + rdc_shipping_cost + rental_cost + in_handling_cost + \
                                        out_handling_cost + reverse_cost + reverse_rental_cost + reverse_in_handling_cost + \
                                        reverse_out_handling_cost
            if self.config.distribute_ToB is True:
                performance['Trunk_cost'] = shipping_cost_trunk
                performance['Transit_cost'] = shipping_cost_transit
                performance['Dist_cost'] = shipping_cost_dist
            performance['rdc_area'] = area
            performance['reverse_area'] = reverse_area
            performance['rdc_weight'] = sum(rdc_weight_d.values())
            performance['reverse_weight'] = sum(reverse_weight_d.values())
            performance['rdc_shipping_distance'] = rdc_shipping_distance
            performance['reverse_shipping_distance'] = reverse_distance
        else:
            performance['total_cost'] = cdc_shipping_cost + rdc_shipping_cost + rental_cost + in_handling_cost + out_handling_cost
            if self.config.distribute_ToB is True:
                performance['Trunk_cost'] = shipping_cost_trunk
                performance['Transit_cost'] = shipping_cost_transit
                performance['Dist_cost'] = shipping_cost_dist
            performance['rdc_area'] = area
            performance['weight'] = sum(rdc_weight_d.values())
            performance['rdc_shipping_distance'] = rdc_shipping_distance

        df = pd.DataFrame()
        rdc_output={}
        m = 0
        for rdc_name in self.rdc:
            rdc_output['RDC'] = rdc_name
            rdc_output['City_name'] = self.GIS[rdc_name]['city_name_cn']
            if self.config.reverse_compute is False:
                rdc_output['CDC_Shipping_Cost'] = cdc_shipping_cost_d[rdc_name]
                rdc_output['Shipping_Cost'] = rdc_shipping_cost_d[rdc_name]
                rdc_output['Storage_Cost'] = rental_cost_d[rdc_name]
                rdc_output['Inbound_Handling_Cost'] = in_handling_cost_d[rdc_name]
                rdc_output['Outbound_Handling_Cost'] = out_handling_cost_d[rdc_name]
                #rdc_output['Inventory'] =
                rdc_output['Total_Cost'] = rdc_output['Shipping_Cost'] + rdc_output['Storage_Cost'] + \
                                           rdc_output['Inbound_Handling_Cost'] + rdc_output['Outbound_Handling_Cost'] +\
                                           rdc_output['CDC_Shipping_Cost']
                rdc_output['Area'] = area_d[rdc_name]
                rdc_output['Weight'] = rdc_weight_d[rdc_name]
                rdc_output['Shipping_Distance']= rdc_shipping_distance_d[rdc_name]
                rdc_output['Customer'] = rdc_count_d[rdc_name]
                rdc_output['Ship_Avg_Price'] = rdc_output['Shipping_Cost'] / rdc_output['Weight']
                rdc_output['Avg_Cost'] = rdc_output['Total_Cost'] / rdc_output['Weight']
            else:
                rdc_output['CDC_Shipping_Cost'] = cdc_shipping_cost_d[rdc_name]
                if config.distribute_ToB:
                    rdc_output['Shipping_Cost'] = rdc_shipping_cost_d[rdc_name]['cost']
                else:
                    rdc_output['Shipping_Cost'] = rdc_shipping_cost_d[rdc_name]
                rdc_output['Reverse_Cost'] = reverse_cost_d[rdc_name]
                rdc_output['Storage_Cost'] = rental_cost_d[rdc_name]
                rdc_output['Reverse_Storage_Cost'] = reverse_rental_cost_d[rdc_name]

                rdc_output['Inbound_Handling_Cost'] = in_handling_cost_d[rdc_name]
                rdc_output['Reverse_Inbound_Handling_Cost'] = reverse_in_handling_cost_d[rdc_name]
                rdc_output['Outbound_Handling_Cost'] = out_handling_cost_d[rdc_name]
                rdc_output['Reverse_Outbound_Handling_Cost'] = reverse_out_handling_cost_d[rdc_name]
                #rdc_output['Inventory'] =
                rdc_output['Total_Cost'] = rdc_output['Shipping_Cost'] + rdc_output['Storage_Cost'] + rdc_output['Inbound_Handling_Cost'] + \
                                           rdc_output['Outbound_Handling_Cost'] + rdc_output['Reverse_Cost'] + rdc_output['Reverse_Storage_Cost'] + \
                                           rdc_output['Reverse_Inbound_Handling_Cost'] + rdc_output['Reverse_Outbound_Handling_Cost'] + rdc_output['CDC_Shipping_Cost']
                rdc_output['Area'] = area_d[rdc_name]
                rdc_output['Reverse_Area'] = reverse_area_d[rdc_name]
                rdc_output['Weight'] = rdc_weight_d[rdc_name]
                rdc_output['Reverse_Weight'] = reverse_weight_d[rdc_name]
                rdc_output['Shipping_Distance']= rdc_shipping_distance_d[rdc_name]
                rdc_output['Reverse_Shipping_Distance'] = reverse_distance_d[rdc_name]
                rdc_output['Customer'] = rdc_count_d[rdc_name]
                rdc_output['Reverse_Customer'] = reverse_count_d[rdc_name]
                rdc_output['Ship_Avg_Price'] = (rdc_output['Shipping_Cost'] + rdc_output['Reverse_Cost'])/ \
                                               (rdc_output['Weight'] + rdc_output['Reverse_Weight'])
                rdc_output['Avg_Cost'] = rdc_output['Total_Cost'] / (rdc_output['Weight'] + rdc_output['Reverse_Weight'])
            # if rdc_output['Quantity'] > 0:
            #     rdc_output['Price_avg'] = rdc_output['Total_Cost'] / rdc_output['Quantity']
            #     rdc_output['shipment_avg'] = rdc_output['shipping_cost'] / rdc_output['Quantity']
            # rdc_output['Volume'] = v_rdc[rdc_name]

            df = df.append(pd.DataFrame(rdc_output, index=[m]))
            m = m + 1

        return pd.DataFrame(performance, index=[0]), df

    def handle_cdcwork(self):
        """
        处理供应商供货情况，明示各SKU供应商来源
        """
        cdc_category_capacity_df = pd.DataFrame(self.cdc_category_capacity).T
        cdc_category_capacity_d =dict()
        for rdc in self.rdc:
            rdc_category_capacity=cdc_category_capacity_df.iloc[[flow[1] == rdc for flow in cdc_category_capacity_df.index],]
            rdc_cn_name = self.GIS[rdc]['city_name_cn']
            cdc_category_capacity_d[rdc_cn_name] = dict()
            for sku in self.category_list:
                for flow,row in rdc_category_capacity.iterrows():
                    cdc_cn_name = self.GIS[flow[0]]['city_name_cn']
                    if row[sku.lower()] > 0:
                        sku_name = self.sku[sku.lower()]['name']
                        cdc_category_capacity_d[rdc_cn_name][sku_name] = (cdc_cn_name,int(round(row[sku.lower()])))

        df_handle_cdc_work = pd.DataFrame(cdc_category_capacity_d)

        factory_unuse = set(self.factory_total) - set(self.cdc_cand)
        factory_unuse_d = {factory: self.GIS[factory]['city_name_cn'] for factory in factory_unuse}
        factory_unuse_df = pd.DataFrame(factory_unuse_d, index=[0])

        return df_handle_cdc_work,factory_unuse_df

    def networkcivilisation(self):
        """
        画出仓网覆盖现状
        """
        # 获取RDC仓位置和城市经纬度坐标点
        RDC_cords = {self.GIS[rdc]['city_name_cn'].split("市")[0] + '仓' : (self.GIS[rdc]['lng'], self.GIS[rdc]['lat']) \
                for rdc in self.rdc}
        City_cords = {customer: (self.GIS[customer]['lng'],self.GIS[customer]['lat']) for customer in self.customer}

        # 初始化地图
        c = Geo(init_opts=opts.InitOpts(width='1500px', height='700px'))
        c.add_schema(
            maptype="china",
            itemstyle_opts=opts.ItemStyleOpts(color="#F0CA00", border_color="#111"))

        # 所有城市、RDC仓的坐标加进地图中
        for key,value in RDC_cords.items():
            c.add_coordinate(key, value[0], value[1])
        for key,value in City_cords.items():
            c.add_coordinate(key, value[0], value[1])

        # 地图中加RDC仓的点
        c.add("RDC仓",
              [[z,1] for z in RDC_cords.keys()],
              type_="scatter", label_opts=opts.LabelOpts(formatter="{b}", color='red', font_size=16, font_weight='bold',\
                                                         position = 'right')
              , color='#3481B8', symbol='pin',
              # 标记图形形状,circle，pin，rect，diamon，roundRect，arrow，triangle
              symbol_size=16
              )

        #加需求点
        non_rdc_customer= set(self.customer) - set(self.rdc)
        c.add("需求点",
              [[z,1] for z in non_rdc_customer],
              type_="scatter", label_opts=opts.LabelOpts(is_show=False)
              , color='blue', blur_size=1, symbol_size=5)

        # 循环添加每一个覆盖
        colors = ["#F70000", "#373F43", "#0A0A0D", "#3E753B", "#3481B8", "#904684", "#D47479", "#a21c68", "#e38105",
                  "#42d1bd"]
        m=0
        for rdc in self.rdc:
            flow=[]
            for key,value in self.demand.items():
                if key[0] == rdc:
                    flow.append((key[0], key[1]))
            c.add(series_name=self.GIS[rdc]['city_name_cn'].split('市')[0] + '仓', data_pair=flow, type_=GeoType.LINES,
                  linestyle_opts=opts.LineStyleOpts(width=0.8, color=colors[m]),
                  label_opts=opts.LabelOpts(is_show=False),
                  ##trail_length=0,
                  is_polyline=True
                  )
            m += 1

        c.set_global_opts(title_opts=opts.TitleOpts(title="现状仓网覆盖"))
        c.render("现状仓网覆盖.html")


if __name__ == "__main__":
    filename = 'data/current_status_input3.0.xlsx'
    config=Config()
    current_status = FacilitylocationCurrentstatus(filename,config)

    filepath = 'to_C_new_demand_results_2020'
    if not os.path.exists(filepath):
        os.mkdir(filepath)

    performance, rdc_output = current_status.resultformat()
    handle_cdc_work,unuse_factory = current_status.handle_cdcwork()
    #current_status.networkcivilisation()

    performance.to_csv('{}/performance.csv'.format(filepath),index=False)
    rdc_output.to_csv('{}/RDC.csv'.format(filepath),encoding = 'utf-8_sig',index=False)
    handle_cdc_work.to_csv('{}/handle_cdc_work.csv'.format(filepath),encoding = 'utf-8_sig',index=False)
    unuse_factory.to_csv('{}/factory_unuse.csv'.format(filepath),encoding = 'utf-8_sig',index=False)

    print("运行结束："+str(datetime.datetime.now()))




