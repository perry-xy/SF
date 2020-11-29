# -*- coding: utf-8 -*-
"""
Created on Tue Oct 13 21:23:19 2020

@author: Administrator
"""
from gurobipy import *

class FacilityLocation():
    """
         Facility Loction model
                                 """

    def __init__(self, data_input, config):
        self.data_input = data_input
        self.config = config
        self.model = None
        self.rdc_open = None
        self.cdc_open = None
        self.r_c = None
        self.cdc_rdc_category = None

    def facility_location(self):
        """
            建模
                 """
        """定义参数"""
        # 必须用的cdc、rdc
        if self.config.rdc_use_constr_open:
            rdc_use=self.config.rdc_use
        if self.config.cdc_use_constr_open:
            cdc_use=self.config.cdc_use
        # 需求点、候选rdc、cdc
        customer = self.data_input.customer
        rdc_cand = self.data_input.rdc_cand
        cdc_cand = self.data_input.cdc_cand
        cdc_category_capacity = self.data_input.cdc_category_capacity
        # 干线运输价格、支线运输价格、sku类别、需求点数据、指定rdc仓数
        trunk_price = self.data_input.trunk_price
        distribution_price = self.data_input.distribution_price
        demand = self.data_input.demand
        category_list = self.data_input.category_list
        num_rdc = self.config.num_rdc

        """定义模型"""
        model = Model('facility_location_WL')

        """定义变量"""
        # cdc的0、1变量
        cdc_open = model.addVars(cdc_cand, vtype=GRB.BINARY, name='cdc_open')
        # rdc的0、1变量
        rdc_open = model.addVars(rdc_cand, vtype=GRB.BINARY, name='rdc_open')
        # rdc与需求点之间的覆盖关系，0、1变量
        r_c = model.addVars(rdc_cand, customer, vtype=GRB.BINARY, name='rdc_customer_category')
        # cdc给rdc供某种货物的量
        cdc_rdc_category = model.addVars(cdc_cand, rdc_cand, category_list, vtype=GRB.CONTINUOUS,
                                         name='cdc_rdc_category')
        # 时效变量
        #sla_c = model.addVars(customer, vtype=GRB.BINARY, name='sla_customer')

        """定义约束条件"""
        # 必选仓必须被选中
        if self.config.cdc_use_constr_open:
            model.addConstrs((cdc_open[cdc_name] == 1 for cdc_name in cdc_use), name='cdc_current_constr')
        if self.config.rdc_use_constr_open:
            model.addConstrs((rdc_open[rdc_name] == 1 for rdc_name in rdc_use), name='rdc_current_constr')
        # 选中仓的数量与规定数目一样
        model.addConstr(sum(rdc_open[rdc_name] for rdc_name in rdc_cand) == num_rdc, name='num_rdc')
        # 一个需求点被且仅被一个rdc仓覆盖
        model.addConstrs((r_c.sum('*', name) == 1 for name in customer), name='demand')
        for rdc_name in rdc_cand:
            model.addConstrs((r_c[rdc_name, name] <= rdc_open[rdc_name] for name in customer), name='r_c_cons')
        # 每个rdc需要的各类别货物的量固定
        customer_category_demand = dict()
        for rdc_name in rdc_cand:
            for category in category_list:
                customer1 = 0
                for name in customer:
                    customer1 += r_c[rdc_name, name] * demand[name][category]
                customer_category_demand[rdc_name, category] = customer1  # 各rdc需要的各类别的sku的量
        for rdc_name in rdc_cand:
            for category in category_list:
                model.addConstr(cdc_rdc_category.sum('*', rdc_name, category) -
                                customer_category_demand[rdc_name, category] == 0, name='demand_cons')
        # 每个cdc供出的货物小于其产能
        for cdc_name in cdc_cand:
            for category in category_list:
                model.addConstr(cdc_rdc_category.sum(cdc_name, "*", category)
                                <= cdc_category_capacity[cdc_name][category] * cdc_open[cdc_name], name='cdc_capacity')
                # model.addConstr(cdc_rdc_category.sum(cdc_name, "*", category)
                #                 >= cdc_category_capacity[cdc_name][category] * (cdc_open[cdc_name]-1) + 1, name='cdc_capacity_1')
        # 若考虑时效，设48小时时效达成率大于90%
        # penality = 200
        # for customer in self.data_input.customer:
        #     model.addConstr(sum(r_c[rdc,customer] * (self.data_input.distribution_price[rdc,customer]['median_time'] - \
        #                                              self.data_input.distribution_price[rdc,customer]['sla']) \
        #                        for rdc in self.data_input.rdc_cand)  <= (1-sla_c[customer]) * penality , name='sla_constr_1')
        #     model.addConstr(sum(r_c[rdc, customer] * (self.data_input.distribution_price[rdc, customer]['median_time'] - \
        #                                               self.data_input.distribution_price[rdc, customer]['sla']) \
        #                         for rdc in self.data_input.rdc_cand) >= - sla_c[customer] * penality , name='sla_constr_2')
        # 若时效达成率为流向个数
        # model.addConstr(sum(sla_c[customer] for customer in self.data_input.customer) >= 0.8 * len(self.data_input.customer),
        #                 name = 'sla_getrate')
        # 若时效达成率以货物重量为权重
        # model.addConstr(sum(sla_c[customer] * self.data_input.demand[customer]['weight'] for customer in self.data_input.customer)\
        #                 - 0.8 * sum(self.data_input.demand[customer]['weight'] for customer in self.data_input.customer), \
        #                 name='sla_constr_3')

        # 若考虑仓库面积上限
        # for rdc in rdc_cand:


        """定义目标函数"""
        cdc_shipping_cost, cdc_shipping_cost_d = self.cdc_shipping_cost(cdc_rdc_category)
        rdc_shipping_cost, rdc_shipping_cost_d, rdc_shipping_distance, shipping_diistance_d = self.rdc_shipping_cost(r_c)
        rdc_storage_cost, rdc_storage_cost_d = self.rdc_storage_cost(r_c)
        rdc_inbound, rdc_outbound, rdc_inbound_cost, rdc_outbound_cost = self.rdc_handling_cost(r_c)
        reverse_cost, reverse_distance, reverse_cost_d, reverse_distance_d = self.reverse_cost(r_c)

        distance_penalty = 100
        cost = cdc_shipping_cost + rdc_shipping_cost + rdc_storage_cost + rdc_inbound + rdc_outbound + reverse_cost
        distance = (rdc_shipping_distance + reverse_distance) * distance_penalty
        # set the Objectice function
        model.setObjective(cost+distance,sense=GRB.MINIMIZE)
        # model.setObjectiveN(cost+100*distance, priority=2, index=0)
        # model.setObjectiveN(distance, priority=1, index=1)

        """求解模型"""
        model.optimize()
        self.model = model
        #self.cdc_open = model.getAttr('x', cdc_open)
        #print(self.cdc_open)
        self.rdc_open = model.getAttr('x', rdc_open)
        self.cdc_rdc_category = model.getAttr('x', cdc_rdc_category)
        self.cdc_open = {cand[0] for cand,value in self.cdc_rdc_category.items() if value > 0}
        print("共有{}个供应商被选择".format(len(self.cdc_open)))
        self.r_c = model.getAttr('x', r_c)

        return model

    def rdc_shipping_cost(self, r_c):
        """
            支线运输成本、距离
                              """

        # 仓 → 目的地中转场 → 中转 → 支线
        # 参数
        rdc_cand = self.data_input.rdc_cand
        customer = self.data_input.customer
        demand = self.data_input.demand
        trunk = self.data_input.trunk_price
        transit = self.data_input.dis_price_toB_transit
        dist = self.data_input.dis_price_toB_dist

        # 计算变量
        shipping_cost_d = dict()  # 各rdc的配送花费
        shipping_cost_trunk = 0  # 支线干线配送成本
        shipping_cost_transit = 0  # 支线中转配送成本
        shipping_cost_dist = 0  # 支线支线配送成本
        shipping_cost = 0 #支线配送总成本
        shipping_diistance_d = dict() #各rdc的配送距离
        shipping_distance = 0  # 总配送距离

        for rdc_name in rdc_cand:
            trunk = 0
            transit = 0
            dist = 0
            cost = 0
            distance = 0
            for name in customer:
                trunk += r_c[rdc_name, name] * demand[name]['weight'] * trunk[rdc_name, name]['trans_fee']
                transit += r_c[rdc_name, name] * demand[name]['weight'] * transit[rdc_name, name]['transit_fee']
                dist += r_c[rdc_name, name] * demand[name]['weight'] * dist[rdc_name]['dist_fee']
                distance += r_c[rdc_name, name] * trunk[rdc_name, name]['distance']
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
            shipping_diistance_d[rdc_name] = distance

        return shipping_cost, shipping_cost_trunk, shipping_cost_transit, shipping_cost_dist, shipping_cost_d, \
               shipping_distance, shipping_diistance_d

    def cdc_shipping_cost(self, cdc_rdc_category):
        """
            干线运输成本
                              """

        # 参数
        rdc_cand = self.data_input.rdc_cand
        trunk_price = self.data_input.trunk_price
        cdc_rand = self.data_input.cdc_cand
        category_list = self.data_input.category_list
        # 计算变量
        shipping_cost = 0
        shipping_cost_d = dict()
        for cdc_name in cdc_rand:
            ship_cdc_rdc = 0
            for rdc_name in rdc_cand:
                for category in category_list:
                    ship_cdc_rdc += cdc_rdc_category[cdc_name, rdc_name, category] * \
                                    trunk_price[cdc_name, rdc_name]['trans_fee']
                    #干线价格若为首、续重的情况
                    #ship_cdc_rdc += distribution_price[cdc_name,rdc_name]['base_price'] + \
                    # (cdc_rdc_category[cdc_name,rdc_name,category]-distribution_price[cdc_name,rdc_name]['base_qty']) * \
                    # distribution_price[cdc_name,rdc_name]['weight_price_qty']
            shipping_cost_d[cdc_name] = ship_cdc_rdc
            shipping_cost += ship_cdc_rdc

        return shipping_cost, shipping_cost_d

    def rdc_storage_cost(self, r_c):
        """
           仓库租金
                    """
        # 参数
        rdc_cand = self.data_input.rdc_cand
        customer = self.data_input.customer
        demand = self.data_input.demand
        category_list = self.data_input.category_list
        warehouse = self.data_input.warehouse
        sku = self.data_input.sku
        # 计算变量
        rdc_storage_cost = 0
        rdc_storage_cost_d = dict()

        for rdc_name in rdc_cand:
            rdc_storage_c_sum = 0
            for name in customer:
                customer_area = 0
                for category in category_list:
                    customer_area += demand[name][category] / \
                                     self.config.YEAR_DAY * sku[category]['turnoverdays'] * sku[category]['area_weight_ratio']

                rdc_storage_c_sum += r_c[rdc_name, name] * self.config.area_ratio * customer_area * 12 * \
                                     warehouse[rdc_name]['rental']
            rdc_storage_cost += rdc_storage_c_sum
            rdc_storage_cost_d[rdc_name] = rdc_storage_c_sum

        return rdc_storage_cost, rdc_storage_cost_d


    def rdc_handling_cost(self, r_c):
        """
           操作费用
                    """
        # 参数
        rdc_cand = self.data_input.rdc_cand
        customer = self.data_input.customer
        demand = self.data_input.demand
        category_list = self.data_input.category_list
        warehouse = self.data_input.warehouse
        # 计算变量
        rdc_inbound = 0
        rdc_outbound = 0
        rdc_inbound_d = dict()
        rdc_outbound_d = dict()

        for rdc_name in rdc_cand:
            inbound_c = 0
            outbound_c = 0
            for name in customer:
                customer_weight = r_c[rdc_name, name] * demand[name]['weight']
                inbound_c += customer_weight * warehouse[rdc_name]['ware_in_fee']
                outbound_c += customer_weight * warehouse[rdc_name]['ware_out_fee']

            rdc_inbound_d[rdc_name] = inbound_c
            rdc_outbound_d[rdc_name] = outbound_c
            rdc_inbound += inbound_c
            rdc_outbound += outbound_c

        return rdc_inbound, rdc_outbound, rdc_inbound_d, rdc_outbound_d

    def reverse_cost(self,r_c):
        """
        调拨费用
        """
        # 参数
        rdc_cand = self.data_input.rdc_cand
        reverse_customer = self.data_input.reverse_customer
        reverse_tob = self.data_input.reverse_tob
        reverse_price = self.data_input.reverse_price

        # 计算变量
        reverse_cost = 0
        reverse_cost_d = dict()
        reverse_distance = 0
        reverse_distance_d = dict()

        for rdc_name in rdc_cand:
            rdc_reverse_cost = 0
            rdc_reverse_distance = 0
            for name in reverse_customer:
                vehicle = reverse_tob[name]['vehicle']
                rdc_reverse_cost += r_c[rdc_name,name] * \
                                    reverse_tob[name]['weight'] * reverse_price[(rdc_name,name,vehicle)]['trans_fee']
                rdc_reverse_distance += r_c[rdc_name,name] * reverse_price[(rdc_name,name,vehicle)]['distance']
            reverse_cost += rdc_reverse_cost
            reverse_distance += rdc_reverse_distance
            reverse_cost_d[rdc_name] = rdc_reverse_cost
            reverse_distance_d[rdc_name] = rdc_reverse_distance

        return reverse_cost, reverse_distance, reverse_cost_d, reverse_distance_d