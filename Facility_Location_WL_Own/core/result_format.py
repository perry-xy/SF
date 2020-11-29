import pandas as pd

class ResultFormat():
    """
    结果输出
    """
    def __init__(self,facility_location_model):
        """
        利用模型结果输出文件
        """
        self.model_result=facility_location_model

    def rdc_post_process(self):
        """
        RDC相关情况输出
        """
        rdc_output = {}
        df = pd.DataFrame()

        warehouse = self.model_result.data_input.warehouse
        customer = self.model_result.data_input.customer
        reverse_customer = self.model_result.data_input.reverse_customer
        demand = self.model_result.data_input.demand
        reverse_demand = self.model_result.data_input.reverse_tob
        city_add = self.model_result.data_input.GIS
        category_list = self.model_result.data_input.category_list
        sku = self.model_result.data_input.sku
        r_c = self.model_result.r_c
        r_r_c = self.model_result.r_r_c
        rdc_open = self.model_result.rdc_open
        cdc_rdc_category = self.model_result.cdc_rdc_category
        #被选中仓
        rdc_open_valid = {k: v for k, v in rdc_open.items() if v > 0.5}

        shipping_cost, shipping_cost_trunk, shipping_cost_transit, shipping_cost_dist, rdc_shipping_cost_d, \
        shipping_distance, shipping_diistance_d = self.model_result.rdc_shipping_cost(r_c)
        # rdc_shipping_cost, rdc_shipping_cost_d, rdc_shipping_distance, shipping_diistance_d = \
        #                                                                 self.model_result.rdc_shipping_cost(r_c)
        rdc_storage_cost, rdc_storage_cost_d, rdc_area, rdc_area_d = self.model_result.rdc_storage_cost(r_c,r_r_c)
        rdc_inbound, rdc_outbound, rdc_inbound_d, rdc_outbound_d = self.model_result.rdc_handling_cost(r_c,r_r_c)
        reverse_cost, reverse_distance, reverse_cost_d, reverse_distance_d = self.model_result.reverse_cost(r_r_c)
        cdc_shipping_cost, cdc_shipping_cost_d = self.model_result.cdc_shipping_cost(cdc_rdc_category)

        m = 0
        for rdc_name in rdc_open_valid:
            rdc_output['RDC'] = rdc_name
            rdc_output['City_name'] = city_add[rdc_name]['city_name_cn']
            rdc_output['CDC_Shipping_Cost'] = cdc_shipping_cost_d[rdc_name]
            rdc_output['Shipping_Cost'] = rdc_shipping_cost_d[rdc_name]['cost']
            rdc_output['Trunk_Cost'] = rdc_shipping_cost_d[rdc_name]['trunk']
            rdc_output['Transit_Cost'] = rdc_shipping_cost_d[rdc_name]['transit']
            rdc_output['Dist_Cost'] = rdc_shipping_cost_d[rdc_name]['dist']
            rdc_output['Reverse_Cost'] = reverse_cost_d[rdc_name]
            rdc_output['Storage_Cost'] = rdc_storage_cost_d[rdc_name]
            rdc_output['Inbound_Handling_Cost'] = rdc_inbound_d[rdc_name]
            rdc_output['Outbound_Handling_Cost'] = rdc_outbound_d[rdc_name]
            rdc_output['Inventory'] = sum(r_c[rdc_name, name] * demand[name][category] / self.model_result.config.YEAR_DAY
                                          * sku[category]['turnoverdays']
                                          #+ category_info[category]['safety_inventory']
                                          for name in customer for category in category_list) \
                                    + sum(r_r_c[rdc_name, name] * reverse_demand[name][category] / self.model_result.config.YEAR_DAY
                                          * sku[category]['turnoverdays']
                                          #+ category_info[category]['safety_inventory']
                                          for name in reverse_customer for category in category_list)
            rdc_output['Area'] = rdc_area_d[rdc_name]
                # sum((r_c[rdc_name, name] * demand[name][category] / self.model_result.config.YEAR_DAY
                #                       * sku[category]['turnoverdays']) * sku[category]['area_weight_ratio']
                                      #+ category_info[category]['safety_inventory']) * category_info[category]['area']
                                     # * self.model_result.config.area_ratio
                                     # for name in customer for category in category_list)
            # rdc_output['Capital_Cost'] = rdc_capital_cost_d[rdc_name]
            rdc_output['Weight'] = sum(r_c[rdc_name, name] * demand[name]['weight']
                                         for name in customer) + sum(r_r_c[rdc_name, name] * reverse_demand[name]['weight']
                                         for name in reverse_customer)
            rdc_output['Total_Cost'] = rdc_output['Shipping_Cost'] + rdc_output['Storage_Cost'] + \
                                       rdc_output['Inbound_Handling_Cost'] + rdc_output['Outbound_Handling_Cost'] + \
                                       rdc_output['Reverse_Cost']
            rdc_output['Shipping_Distance'] = shipping_diistance_d[rdc_name]
            rdc_output['Reverse_Shipping_Distance'] = reverse_distance_d[rdc_name]
            rdc_output['Normal_Customer']=sum(r_c[rdc_name, name] for name in customer)
            rdc_output['Reverse_Customer']=sum(r_r_c[rdc_name,name] for name in reverse_customer)
            try:
                rdc_output['Ship_Avg_Price'] = (rdc_output['Shipping_Cost'] +rdc_output['Reverse_Cost']) /rdc_output['Weight']
            except:
                rdc_output['Ship_Avg_Price'] = 0
            try:
                rdc_output['Avg_Cost'] = rdc_output['Total_Cost']/rdc_output['Weight']
            except:
                rdc_output['Avg_Cost'] = 0
            # if rdc_output['Quantity'] > 0:
            #     rdc_output['Price_avg'] = rdc_output['Total_Cost'] / rdc_output['Quantity']
            #     rdc_output['shipment_avg'] = rdc_output['shipping_cost'] / rdc_output['Quantity']
            # rdc_output['Volume'] = v_rdc[rdc_name]

            df = df.append(pd.DataFrame(rdc_output, index=[m]))
            m = m + 1

        return df

    def c_end_network(self):
        """
        RDC与需求点网络关系
        """

        customer = self.model_result.data_input.customer
        reverse_customer = self.model_result.data_input.reverse_customer
        demand = self.model_result.data_input.demand
        reverse_demand = self.model_result.data_input.reverse_tob
        city_add = self.model_result.data_input.GIS

        c_network = dict()
        rdc_open = self.model_result.rdc_open

        # 被选中仓
        rdc_open_valid = {k: v for k, v in rdc_open.items() if v > 0.5}
        r_c = self.model_result.r_c
        r_r_c = self.model_result.r_r_c
        sku = self.model_result.data_input.sku

        k = 0
        df_c = pd.DataFrame()

        for rdc_name in rdc_open_valid.keys():
            c_network['RDC'] = rdc_name
            c_network['RDC_NAME'] = city_add[rdc_name]['city_name_en']
            c_network['RDC_LAT'] = city_add[rdc_name]['lat']
            c_network['RDC_LGT'] = city_add[rdc_name]['lng']
            for c_name in customer:
                if r_c[rdc_name, c_name] >= 0.90:
                    c_network['CUSTOMER'] = c_name
                    #c_network['QUANTITY'] = f_c[rdc_name, c_name] * demand[c_name]['demand_sum']

                    c_network['WEIGHT'] = r_c[rdc_name, c_name] * demand[c_name]['weight']
                    # c_network['SLA'] = f_c[rdc_name, c_name] * distribution_price[rdc_name, c_name]['sla_toC']
                    c_network['CUSTOMER_LGT'] = city_add[c_name]['lng']
                    c_network['CUSTOMER_LAT'] = city_add[c_name]['lat']
                    c_network['CUSTOMER_NAME'] = city_add[c_name]['city_name_en']
                    c_network['TYPE'] = 'Normal'
                    c_network['Vehicle'] = None
                    # c_network['TIME'] = distribution_price[rdc_name, c_name]['time_median_toC']
                    df_c = df_c.append(pd.DataFrame(c_network, index=[k]))
                    k = k+1
        for rdc_name in rdc_open_valid.keys():
            c_network['RDC'] = rdc_name
            c_network['RDC_NAME'] = city_add[rdc_name]['city_name_en']
            c_network['RDC_LAT'] = city_add[rdc_name]['lat']
            c_network['RDC_LGT'] = city_add[rdc_name]['lng']
            for c_name in reverse_customer:
                if r_r_c[rdc_name, c_name] >= 0.90:
                    c_network['CUSTOMER'] = c_name
                    #c_network['QUANTITY'] = f_c[rdc_name, c_name] * demand[c_name]['demand_sum']

                    c_network['WEIGHT'] = r_r_c[rdc_name, c_name] * reverse_demand[c_name]['weight']
                    # c_network['SLA'] = f_c[rdc_name, c_name] * distribution_price[rdc_name, c_name]['sla_toC']
                    c_network['CUSTOMER_LGT'] = city_add[c_name]['lng']
                    c_network['CUSTOMER_LAT'] = city_add[c_name]['lat']
                    c_network['CUSTOMER_NAME'] = city_add[c_name]['city_name_en']
                    c_network['TYPE'] = 'Reverse'
                    c_network['Vehicle'] = reverse_demand[c_name]['vehicle']
                    # c_network['TIME'] = distribution_price[rdc_name, c_name]['time_median_toC']
                    df_c = df_c.append(pd.DataFrame(c_network, index=[k]))
                    k = k+1

        return df_c

    def cdc_rdc_network(self):
        """
        CDC与RDC网络关系
        """
        cdc_cand = self.model_result.data_input.cdc_cand
        city_add = self.model_result.data_input.GIS
        category_list=self.model_result.data_input.category_list

        rdc_open = self.model_result.rdc_open
        # filter all items that cdc_open = 1, get the valid
        rdc_open_valid = {k: v for k, v in rdc_open.items() if v > 0.5}
        cdc_rdc_category = self.model_result.cdc_rdc_category

        k = 0
        df_c = pd.DataFrame()
        for cdc_name in cdc_cand:
            for rdc_name in rdc_open_valid.keys():
                category_demand_list = []
                for category in category_list:
                    if cdc_rdc_category[cdc_name, rdc_name, category] > 0:
                        category_demand_list.append(cdc_rdc_category[cdc_name, rdc_name, category])
                    else:
                        category_demand_list.append(0)
                if sum(category_demand_list)>0:
                    category_demand_list = [cdc_name, city_add[cdc_name]['lat'], city_add[cdc_name]['lng'],
                                            rdc_name, city_add[rdc_name]['lat'], city_add[rdc_name]['lng']]\
                                           + category_demand_list
                    df_c = df_c.append([category_demand_list])
                else:
                    continue
        df_c.columns = ['CDC_Name', 'CDC_LAT', 'CDC_LGT', 'RDC_Name','RDC_LAT', 'RDC_LGT'] \
                        + [z.upper() for z in category_list]
        return df_c

    def handle_cdc_work(self,cdc_category_capacity_df):
        """
        处理供应商网络关系，明示各SKU供货供应商所在地
        """
        cdc_category_capacity_df = cdc_category_capacity_df.set_index(['CDC_Name','RDC_Name'])
        cdc_category_capacity_df.drop(['CDC_LAT','CDC_LGT','RDC_LAT','RDC_LGT'],axis=1)

        rdc_open_valid = {k: v for k, v in self.model_result.rdc_open.items() if v > 0.5}

        cdc_category_capacity_d = dict()
        for rdc in rdc_open_valid.keys():
            rdc_category_capacity = cdc_category_capacity_df.iloc[[flow[1] == rdc for flow in cdc_category_capacity_df.index],]
            rdc_cn_name = self.model_result.data_input.GIS[rdc]['city_name_cn']
            cdc_category_capacity_d[rdc_cn_name] = dict()
            for sku in self.model_result.data_input.category_list:
                for flow, row in rdc_category_capacity.iterrows():
                    cdc_cn_name = self.model_result.data_input.GIS[flow[0]]['city_name_cn']
                    if row[sku.upper()] > 0:
                        sku_name = self.model_result.data_input.sku[sku.lower()]['name']
                        cdc_category_capacity_d[rdc_cn_name][sku_name] = (cdc_cn_name, int(round(row[sku.upper()])))

        df_handle_cdc_work = pd.DataFrame(cdc_category_capacity_d)

        factory_unuse = {factory: self.model_result.data_input.GIS[factory]['city_name_cn'] for factory in \
                         (set(self.model_result.data_input.cdc_cand) - self.model_result.cdc_open)}
        #factory_unuse_list = [self.model_result.data_input.GIS[factory]['city_name_cn'] for factory in factory_unuse]
        factory_unuse_df = pd.DataFrame(factory_unuse,index=[0])

        return df_handle_cdc_work, factory_unuse_df

    def performance(self,m):
        """
        总体表现
        """
        cdc_shipping_cost, cdc_shipping_cost_d = self.model_result.cdc_shipping_cost(self.model_result.cdc_rdc_category)
        shipping_cost, shipping_cost_trunk, shipping_cost_transit, shipping_cost_dist, rdc_shipping_cost_d, \
        shipping_distance, shipping_diistance_d = self.model_result.rdc_shipping_cost(self.model_result.r_c)
        # rdc_shipping_cost, rdc_shipping_cost_d, rdc_shipping_distance, shipping_diistance_d \
        #                                                     = self.model_result.rdc_shipping_cost(self.model_result.r_c)
        rdc_storage_cost, rdc_storage_cost_d,rdc_area,rdc_area_d= self.model_result.rdc_storage_cost(self.model_result.r_c,self.model_result.r_r_c)
        rdc_inbound, rdc_outbound, rdc_inbound_d, rdc_outbound_d = self.model_result.rdc_handling_cost(self.model_result.r_c, \
                                                                                                       self.model_result.r_r_c )
        reverse_cost, reverse_distance, reverse_cost_d, reverse_distance_d = self.model_result.reverse_cost(self.model_result.r_r_c)
        # performance
        performance_output = dict()
        performance_output['RDC'] = self.model_result.config.num_rdc
        performance_output['Trans_cost'] = cdc_shipping_cost
        performance_output['Trunk_cost'] = shipping_cost_trunk
        performance_output['Transit_cost'] = shipping_cost_transit
        performance_output['Dist_cost'] = shipping_cost_dist
        #performance_output['Trunk_cost'] = shipping_cost_trunk
        performance_output['Rdc_cus_cost'] = shipping_cost_trunk + shipping_cost_transit + shipping_cost_dist
        performance_output['Reverse_shipping_cost'] = reverse_cost
        performance_output['Storage_cost'] = rdc_storage_cost
        performance_output['Handling_cost'] = rdc_inbound + rdc_outbound
        performance_output['Total_Cost'] = performance_output['Trans_cost'] + performance_output['Dist_cost'] \
                                           + performance_output['Storage_cost'] + performance_output['Transit_cost'] + \
                                           performance_output['Trunk_cost'] + \
                                           performance_output['Handling_cost'] + performance_output['Reverse_shipping_cost']
        performance_output['Total_Area'] = rdc_area
        performance_output['RDC_Shipping_Distance'] = shipping_distance
        performance_output['RDC_Reverse_Distance'] = reverse_distance

        return pd.DataFrame(performance_output, index=[m])

