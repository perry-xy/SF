#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2019/9/9 9:55
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: model.py
# @ProjectName :Facility_Location_FangTai
"""

from gurobipy import *
from utils.misc import Logger
import pandas as pd
from collections import defaultdict
# define the log file
log = Logger(log_path='../log').logger

# define the facility location problem

YEAR_DAY = 365


class FacilityLocation(object):
    """
    this class is consist of attributes for problem construction
    some utils function for dealing with post-process
    one key function for building the detail model
    """
    def __init__(self, data, config):
        """

        :param data: class of data provide all data used
        :param config: class of config provide all configuration used
        :param model: model of facility location

        """
        self.data_class = data
        self.config = config
        self.model = None
        self.rdc_open = None
        self.f_c = None
        self.cdc_rdc_category = None

    def facility_location(self):
        """
        the function will build a facility location optimization model which take the self.data_class input as parameters.
        return the optimized model
        :return: model
        """
        """######## list the parameters ######### """
        # the current cdc and must use cdc
        cdc_use = self.data_class.cdc_use
        # the current rdc and must use rdc
        rdc_current = self.data_class.rdc_current
        rdc_use = self.data_class.rdc_use
        # candidate list of potential locations of CDC, RDC and customer list
        customer = self.data_class.customer
        cdc_cand = self.data_class.cdc_cand
        cdc_category_capacity = self.data_class.cdc_category_capacity        # cdc capacity
        rdc_capacity = self.data_class.rdc_capacity
        rdc_cand = self.data_class.rdc_cand
        category_info = self.data_class.category_info
        # parameters
        trunk_price = self.data_class.trunk_price
        distribution_price = self.data_class.distribution_price
        demand = self.data_class.demand
        category_list = self.config.category_list            # SKU category
        num_rdc = self.config.num_rdc
        """######## declare the model ######### """
        model = Model('facility_location_WL')

        """######## define the decision variables ######### """
        # the indicator of open status of RDC
        cdc_open = model.addVars(cdc_cand, vtype=GRB.BINARY, name='rdc_open')
        # the indicator of open status of RDC
        rdc_open = model.addVars(rdc_cand, vtype=GRB.BINARY, name='rdc_open')
        # quantity of specific SKU category of demand of customer supplied by RDC
        f_c = model.addVars(rdc_cand, customer, vtype=GRB.BINARY, name='rdc_customer_category')
        # quantity of specific SKU category of demand of RDC supplied by CDC
        cdc_rdc_category = model.addVars(cdc_cand, rdc_cand, category_list, vtype=GRB.CONTINUOUS,
                                         name='cdc_rdc_category')
        # the indicator of reachable of SLA
        # sla_b = model.addVars(customer_2b, vtype=GRB.BINARY, name='sla_b')
        # sla = model.addVars(customer, vtype=GRB.BINARY, name='sla_c')
        """######## define the constrains ######### """
        # the current of cdc and rdc should be used constraints
        if self.config.use_location_constr_open:
            model.addConstrs((cdc_open[cdc_name] == 1 for cdc_name in cdc_use), name='cdc_current_constr')
            model.addConstrs((rdc_open[rdc_name] == 1 for rdc_name in rdc_use), name='rdc_current_constr')
        # if self.config.use_location_constr_open:
        #     # rdc_use = ['20', '22', '23', '28', '29', '351', '378', '411', '431',
        #     '451', '471','539','574', '594', '714', '7311',
        #     #            '772','791', '851', '871', '991']
        # rdc_use = ['28', '311','572','760', '711','24','371','7311','29','531']
        #rdc_use = ['28', '311','572','760', '711','24','371']   #7仓
        #rdc_use = ['28', '311','571','760']    #4仓
        # rdc_use = ["22", "24", "572", "769", "27", "28" ]
        rdc_use = ["24", "711", "531", "29", "572", "28","760","311","7311","371"]
        model.addConstrs((rdc_open[rdc_name] == 1 for rdc_name in rdc_use), name='rdc_use_constr')

        # the number of potential location of RDC constraints
        model.addConstr(sum(rdc_open[rdc_name] for rdc_name in rdc_cand) == num_rdc, name='num_rdc')
        # the demand of  customer should be met constrains
        # the demand of  customer should be met constrains
        model.addConstrs((f_c.sum('*', name) == 1 for name in customer), name='demand')
        for rdc_name in rdc_cand:
            model.addConstrs((f_c[rdc_name, name] <= rdc_open[rdc_name] for name in customer), name='f_c_cons')

        customer_category_demand = dict()
        for rdc_name in rdc_cand:
            # rdc_demand_category = 0

            # customer2 = 0
            # customer3 = 0
            # customer4 = 0
            # customer5 = 0
            # customer6 = 0
            for category in category_list:
                customer1 = 0
                for name in customer:
                    customer1 += f_c[rdc_name, name] * demand[name][category + "_weight"]
                # customer6 += f_c[rdc_name, name] * demand[name]['SKU6']
                customer_category_demand[rdc_name, category] = customer1

        for rdc_name in rdc_cand:
            for category in category_list:
                model.addConstr(cdc_rdc_category.sum('*', rdc_name, category) -
                                customer_category_demand[rdc_name, category] == 0, name='demand_cons')

        # for rdc_name in rdc_cand:
        #     customer_demand = 0
        #     for name in customer:
        #         for category in category_list:
        #             customer_demand += f_c[rdc_name, name] * demand[name][category]
        #             model.addConstr(cdc_rdc_category.sum('*', rdc_name, category) - customer_demand == 0, name='demand_cons')

        # model.addConstrs((f_b.sum('*', name) == 1 for name in customer_2b), name='2b_demand')
        # model.addConstrs((f_c.sum('*', name) == 1 for name in customer), name='demand')
        for cdc_name in cdc_cand:
            for category in category_list:
                model.addConstr(cdc_rdc_category.sum(cdc_name, "*", category)
                                <= cdc_open[cdc_name] * cdc_category_capacity[cdc_name][category], name='cdc_capacity')



        # model.update()
            # model.addConstrs((f_b[rdc_name, name] <= rdc_open[rdc_name] for name in customer_2b), name='f_b_cons')
        # # the capacity constrains
        # rr = self.config.rr
        # rdc_capacity = self.data_class.rdc_capacity
        # if self.config.capacity_constrs_open:
        #     for rdc_name in rdc_cand:
        #         model.addConstr(sum((f_c[rdc_name, name] * demand[name]['demand_sum'] * self.config.turnover_days_c
        #                              for name in customer)) +
        #                         sum((f_c[rdc_name, name] * demand[name]['demand_sum'] for name in
        #                              customer)) * rr * self.config.return_turnover_days
        #                     + sum((f_b[rdc_name, name] * demand_2b[name]['demand_sum'] * self.config.turnover_days_b
        #                            for name in customer_2b))
        #                         <= rdc_open[rdc_name] * rdc_capacity[rdc_name], name='capacity_constr')
        #         # model.addConstr(sum((f_c[rdc_name, name] * demand[name]['demand_sum'] * self.config.turnover_days_c
        #         #                      for name in customer)) +
        #         #                 sum((f_c[rdc_name, name] * demand[name]['demand_sum'] for name in
        #         #                      customer)) * rr * self.config.return_turnover_days
        #         #             + sum((f_b[rdc_name, name] * demand_2b[name]['demand_sum'] * self.config.turnover_days_b
        #         #                    for name in customer_2b))
        #         #                 >= rdc_capacity[rdc_name], name='capacity_constr_1')
        #
        # # the toC sla constrains
        # SLA_NUM = 200
        # if self.config.sla_2c_constrs_open:
        #
        #     for name in customer:
        #         model.addConstr(sum(f_c[rdc_name, name] * distribution_price[rdc_name, name]['time_median_toC']
        #                             for rdc_name in rdc_cand)
        #             - sum(f_c[rdc_name, name] * distribution_price[rdc_name, name]['sla_toC']
        #                             for rdc_name in rdc_cand) <= SLA_NUM * (1 - sla[name]), name='sla_constr')
        #         model.addConstr(sum(f_c[rdc_name, name] * distribution_price[rdc_name, name]['time_median_toC']
        #                             for rdc_name in rdc_cand)
        #              - sum(f_c[rdc_name, name] * distribution_price[rdc_name, name]['sla_toC']
        #                             for rdc_name in rdc_cand) >= -SLA_NUM * sla[name], name='sla_constr_1')
        #     model.addConstr(sum(sla[name] * demand[name]['demand_sum'] for name in customer) >=
        #                 self.config.P_c * sum(demand[name]['demand_sum'] for name in customer),
        #                 name='sla_constr_2')
        # # toB sla  constrains
        # if self.config.sla_2b_constrs_open:
        #     for name in customer_2b:
        #         model.addConstr(sum(f_b[rdc_name, name] * distribution_price[rdc_name, name]['time_median_toB']
        #                             for rdc_name in rdc_cand)
        #             - sum(f_b[rdc_name, name] * distribution_price[rdc_name, name]['sla_toB']
        #                             for rdc_name in rdc_cand) <= SLA_NUM * (1 - sla_b[name]), name='sla_constr_2b')
        #         model.addConstr(sum(f_b[rdc_name, name] * distribution_price[rdc_name, name]['time_median_toB']
        #                             for rdc_name in rdc_cand)
        #             - - sum(f_b[rdc_name, name] * distribution_price[rdc_name, name]['sla_toB']
        #                             for rdc_name in rdc_cand) >= -SLA_NUM * sla_b[name], name='sla_constr_2b_1')
        #     model.addConstr(sum(sla_b[name] * demand_2b[name]['demand_sum'] for name in customer_2b) >=
        #                 self.config.P_b * sum(demand_2b[name]['demand_sum'] for name in customer_2b),
        #                 name='sla_constr_2b_2')


        """######## define the objective function ######### """
        # define the shipping cost of cdc
        # intermediate variables
        # v_cdc_rdc, v_cdc = self.cdc_rdc_temp_calc(cdc_rdc_category)
        # i_rdc_C, q_rdc_C, w_rdc_C, i_rdc_c, q_rdc_c, w_rdc_c = self.rdc_c_temp_calc(rdc_customer_category)
        # i_rdc_B, q_rdc_B, w_rdc_B, i_rdc_b, q_rdc_b, w_rdc_b = self.rdc_b_temp_calc(f_b)
        # i_rdc_c, i_rdc, q_rdc_c, d_rdc_c, v_rdc = self.rdc_c_temp_calc_use(f_c)
        cdc_shipping_cost, cdc_shipping_cost_d = self.cdc_shipping_cost(cdc_rdc_category)
        rdc_shipping_cost, rdc_shipping_cost_d, rdc_shipping_distance = self.rdc_shipping_cost(f_c)
        rdc_storage_cost, rdc_storage_cost_d = self.rdc_storage_cost(f_c)
        rdc_inbound, rdc_outbound, rdc_inbound_cost, rdc_outbound_cost = self.rdc_handling_cost(f_c)
        # rdc_capital_cost, rdc_capital_cost_d = self.capital_cost(i_rdc_c)
        # cost = cdc_shipping_cost + cdc_shipping_cost_r + rdc_shipping_cost + rdc_storage_cost\
        #        + rdc_inbound + rdc_outbound + rdc_r_outbound + rdc_capital_cost
        distance_penalty = 0.1
        # time_penalty = 0.1
        # time_used = self.rdc_time_calc(f_c, f_b)
        cost = self.config.trunk_ratio * cdc_shipping_cost + self.config.dist_discount * rdc_shipping_cost \
               + rdc_storage_cost + rdc_inbound + rdc_outbound
        distance =  rdc_shipping_distance * distance_penalty
        # set the Objectice function
        model.setObjectiveN(cost,priority=2,index=0)
        model.setObjectiveN(distance, priority=1,index=1)

        """######## solve the model ######### """
        model.optimize()

        """######## check the model ######### """
        if model.Status == GRB.OPTIMAL:
            log.info('the facility location optimized sucessfully !')
            log.info('the objective of the model is {}'.format(model.objVal))
            self.model = model
            log.info('dump model to file:\n')
            model.write('facility location ft.lp')

            # get all solved variables
            self.cdc_open = model.getAttr('x', cdc_open)
            self.rdc_open = model.getAttr('x', rdc_open)
            self.cdc_rdc_category = model.getAttr('x', cdc_rdc_category)
            self.f_c = model.getAttr('x', f_c)

            return model
        else:
            log.info('the model is infeasible!!!')
            return 0

        # do IIS
        model.computeIIS()
        if model.IISMinimal:
            print('IIS is minimal\n')
        else:
            print('IIS is not minimal \n')
        log.warn('the following constraints cannot be satisfied')
        for c in model.getConstrs():
            if c.IISConstr:
                log.info('%s' % c.constrName)

    def cdc_shipping_cost(self, cdc_rdc_category):
        """
        calculate the shipping cost from cdc to rdc and
        reverse shipping cost from rdc to cdc
        the equation of shipping cost is following :
        tl_v_cdc_rdc = v_cdc_rdc * 30%
        ftl_v_cdc_rdc = v_cdc_rdc * 70% / self.config.volume_per_truck


        :param: cdc_rdc_category the weight of category from cdc to rdc
        :return:
            shipping_cost: shipping cost from cdc to rdc
            shipping_cost_d: shipping cost from cdc to rdc for each cdc
        """

        # parameters
        rdc_cand = self.data_class.rdc_cand
        trunk_price = self.data_class.trunk_price
        cdc_rand = self.data_class.cdc_cand
        category_list = self.config.category_list
        category_info = self.data_class.category_info
        shipping_cost = 0
        shipping_cost_d = {}
        for cdc_name in cdc_rand:
            ship_cdc_rdc = 0
            for rdc_name in rdc_cand:
                for category in category_list:
                    ship_cdc_rdc += cdc_rdc_category[cdc_name, rdc_name, category] \
                                    * trunk_price[cdc_name, rdc_name]['weight_price']

            shipping_cost_d[cdc_name] = ship_cdc_rdc
            shipping_cost += ship_cdc_rdc

        return shipping_cost, shipping_cost_d


    def rdc_shipping_cost(self, f_c):
        """
        calculate the shipping cost from rdc to customer
        reverse from c to rdc is omit now
        :param: rdc_customer_category:
        :return:
        """
        # parameters
        rdc_cand = self.data_class.rdc_cand
        customer = self.data_class.customer
        demand = self.data_class.demand
        category_list = self.config.category_list
        distribution_price = self.data_class.distribution_price
        category_info = self.data_class.category_info
        # declare variables
        shipping_cost_d = {}
        shipping_cost_c = 0
        shipping_distance = 0

        for rdc_name in rdc_cand:
            ship_c = 0
            distance = 0
            for name in customer:
                ship_c += f_c[rdc_name, name] * demand[name]['demand_weight_sum'] / self.config.weight_avg * \
                          ((self.config.weight_avg - distribution_price[rdc_name, name]['base_weight_qty'])
                           * distribution_price[rdc_name, name]['weight_price_qty']
                           + distribution_price[rdc_name, name]['base_price'])
                distance += f_c[rdc_name, name]*distribution_price[rdc_name, name]['distance']
            shipping_cost_d[rdc_name] = ship_c
            # all RDC
            shipping_cost_c += ship_c
            shipping_distance += distance

        return shipping_cost_c, shipping_cost_d, shipping_distance

    # TODO: 确认一下周转库存天数计算公式： 周转天数 = 平均库存/平均需求
    def rdc_storage_cost(self, f_c):
        """
        calculate the storage cost of rdc

        :param: I_rdc_c: the average inventory from rdc to c
        :return:
        """
        # parameters
        rdc_cand = self.data_class.rdc_cand
        customer = self.data_class.customer
        demand = self.data_class.demand
        category_list = self.config.category_list
        rdc_cost_loc = self.data_class.rdc_cost_loc
        category_info = self.data_class.category_info
        # declare variables
        rdc_storage_cost = 0
        rdc_storage_cost_d = {}

        for rdc_name in rdc_cand:
            rdc_storage_c_sum = 0
            for name in customer:
                customer_area = 0
                for category in category_list:
                    customer_area += (demand[name][category] /
                                      YEAR_DAY * category_info[category]['turn_over_day'] +
                                      category_info[category]['safety_inventory']) * category_info[category]['area']

                rdc_storage_c_sum += f_c[rdc_name, name] * (1 + self.config.area_ratio) * customer_area * 12 \
                                     * rdc_cost_loc[rdc_name]['monthly_rental_price']
            rdc_storage_cost += rdc_storage_c_sum
            rdc_storage_cost_d[rdc_name] = rdc_storage_c_sum

        return rdc_storage_cost, rdc_storage_cost_d

    def rdc_handling_cost(self, f_c):
        """
        calculate the handling cost
            inbound cost:
            outbound cost
        :return:
        """
        # parameters
        rdc_cand = self.data_class.rdc_cand
        customer = self.data_class.customer
        demand = self.data_class.demand
        category_list = self.config.category_list
        rdc_cost_loc = self.data_class.rdc_cost_loc
        category_info = self.data_class.category_info
        # declare
        rdc_inbound = 0
        rdc_outbound = 0

        rdc_inbound_cost = {}
        rdc_outbound_cost = {}

        for rdc_name in rdc_cand:
            inbound_c = 0
            outbound_c = 0
            for name in customer:
                customer_weight = f_c[rdc_name, name] * demand[name]['demand_weight_sum']
                inbound_c += customer_weight * rdc_cost_loc[rdc_name]['in_handling_cost']
                outbound_c += customer_weight * rdc_cost_loc[rdc_name]['out_handling_cost']

            rdc_inbound_cost[rdc_name] = inbound_c
            rdc_outbound_cost[rdc_name] = outbound_c
            rdc_inbound += inbound_c
            rdc_outbound += outbound_c
        return rdc_inbound, rdc_outbound, rdc_inbound_cost, rdc_outbound_cost

    def capital_cost(self, i_rdc_c):
        """

        :return:
        """
        # parameters
        rdc_cand = self.data_class.rdc_cand
        customer = self.data_class.customer
        uap = self.config.uap
        rdc_capital_cost_d = {}
        i_sum = 0
        for rdc_name in rdc_cand:
            i_rdc_sum = 0
            for name in customer:
                i_rdc_sum += i_rdc_c[rdc_name, name]
            i_sum += i_rdc_sum
            rdc_capital_cost_d[rdc_name] = i_rdc_sum * uap * self.config.annual_rate
        rdc_capital_cost = i_sum * uap * self.config.annual_rate

        return rdc_capital_cost, rdc_capital_cost_d

    def rdc_time_calc(self, f_c, f_b):
        """

        :return:
        """
        rdc_cand = self.data_class.rdc_cand
        customer = self.data_class.customer
        customer_2b = self.data_class.customer_2b
        distribution_price = self.data_class.distribution_price
        time_used = 0
        for rdc_name in rdc_cand:
            time_used_temp = 0
            for name in customer:
               time_used_temp += f_c[rdc_name, name] * distribution_price[name, rdc_name]['time_median_toC']
            for name_b in customer_2b:
                time_used_temp += f_b[rdc_name, name_b] * distribution_price[name_b, rdc_name]['time_median_toB']
            time_used += time_used_temp
        return time_used

    # the intermediate variables should include v_cdc_rdc, w_rdc_c, i_rdc_c, q_rdc_c
    # TODO: have some problem
    def cdc_rdc_temp_calc(self, q_rdc_C, w_rdc_C, q_rdc_B, w_rdc_B):
        """
        calculate the intermediate variables from cdc to rdc
        :return:
            v_cdc_rdc： the volume of yearly demand of all from cdc to rdc
        """
        q_cdc_rdc = {}
        q_cdc = {}
        w_cdc_rdc = {}
        w_cdc = {}

        # parameters

        rdc_cand = self.data_class.rdc_cand
        cdc_cand = self.data_class.cdc_cand

        for cdc_name in cdc_cand:
            q_cdc_tmp = 0
            w_cdc_tmp = 0
            for rdc_name in rdc_cand:
                q_cdc_tmp += q_rdc_C[rdc_name]['less'] + q_rdc_C[rdc_name]['more'] + q_rdc_B[rdc_name]
                w_cdc_tmp += w_rdc_C[rdc_name] + w_rdc_B[rdc_name]

                q_cdc_rdc[cdc_name, rdc_name] = q_rdc_C[rdc_name]['less'] + q_rdc_C[rdc_name]['more'] + q_rdc_B[rdc_name]
                w_cdc_rdc[cdc_name, rdc_name] = w_rdc_C[rdc_name] + w_rdc_B[rdc_name]
            q_cdc[cdc_name] = q_cdc_tmp
            w_cdc[cdc_name] = w_cdc_tmp

        return q_cdc, w_cdc, q_cdc_rdc, w_cdc_rdc

    def rdc_c_temp_calc(self, f_c):
        """
        calculate the intermedidate variables from rdc to c
        :return:
        """
        # parameters
        rdc_cand = self.data_class.rdc_cand
        customer = self.data_class.customer
        demand = self.data_class.demand
        turnover_days_c = self.config.turnover_days_c
        total_days = 365
        # declare variables
        q_rdc = {}
        w_rdc_c = {}
        w_rdc = {}
        i_rdc_c = {}
        i_rdc = {}
        q_rdc_c = {}

        for rdc_name in rdc_cand:
            q_rdc_less = 0
            q_rdc_more = 0
            w_rdc_less = 0
            w_rdc_more = 0
            i_rdc_sum = 0

            # toC customer
            for name in customer:

                q_rdc_less += f_c[rdc_name, name] * demand[name]['demand_L']
                q_rdc_more += f_c[rdc_name, name] * demand[name]['demand_M']

                w_rdc_less += f_c[rdc_name, name] * demand[name]['demand_L_weight']
                w_rdc_more += f_c[rdc_name, name] * demand[name]['demand_M_weight']

                i_rdc_tmp = (f_c[rdc_name, name] * demand[name]['demand_sum']) / total_days * turnover_days_c
                q_rdc_c[rdc_name, name] = {'less': f_c[rdc_name, name] * demand[name]['demand_L'],
                                           'more': f_c[rdc_name, name] * demand[name]['demand_M']
                                           }
                # TODO: comfirm
                w_rdc_c[rdc_name, name] = f_c[rdc_name, name] * demand[name]['demand_M_avg_weight']

                i_rdc_sum += i_rdc_tmp
                i_rdc_c[rdc_name, name] = i_rdc_tmp
            i_rdc[rdc_name] = i_rdc_sum
            q_rdc[rdc_name] = {'less': q_rdc_less,
                               'more': q_rdc_more,
                               }
            w_rdc[rdc_name] = w_rdc_less + w_rdc_more

        return i_rdc, q_rdc, w_rdc, i_rdc_c, q_rdc_c, w_rdc_c

        # post-process of cdc

    def rdc_b_temp_calc(self, f_b):
        """
        calculate the intermedidate variables from rdc to B

        :return:
        """
        # parameters
        total_days = 365
        rdc_cand = self.data_class.rdc_cand
        customer = self.data_class.customer_2b
        demand = self.data_class.demand_2b
        turnover_days_b = self.config.turnover_days_b

        # declare variables
        q_rdc = {}
        q_rdc_b = {}
        w_rdc_b = {}
        w_rdc = {}
        i_rdc_b = {}
        i_rdc = {}
        for rdc_name in rdc_cand:
            q_rdc_tmp = 0
            w_rdc_tmp = 0
            i_rdc_tmp = 0
            for name in customer:
                q_rdc_tmp += f_b[rdc_name, name] * demand[name]['demand_sum']
                w_rdc_tmp += f_b[rdc_name, name] * demand[name]['demand_weight_sum']
                i_rdc_tmp += (f_b[rdc_name, name] * demand[name]['demand_sum']) / total_days * turnover_days_b

                w_rdc_b[rdc_name, name] = f_b[rdc_name, name] * demand[name]['demand_weight_sum']
                i_rdc_b[rdc_name, name] = (f_b[rdc_name, name] * demand[name]['demand_sum']) / total_days * turnover_days_b
                q_rdc_b[rdc_name, name] = f_b[rdc_name, name] * demand[name]['demand_sum']

            q_rdc[rdc_name] = q_rdc_tmp
            w_rdc[rdc_name] = w_rdc_tmp
            i_rdc[rdc_name] = i_rdc_tmp

        return i_rdc, q_rdc, w_rdc, i_rdc_b, q_rdc_b, w_rdc_b

        # post-process of cdc

    def cdc_post_process(self):
        """
        doing the post process of cdc, output the detail quantity of each items
        :return:

        """
        cdc_output = {}
        df = pd.DataFrame()
        cdc_cand = self.data_class.cdc_cand
        rdc_cand = self.data_class.rdc_cand
        cdc_rdc_category = self.cdc_rdc_category
        category_list = self.config.category_list
        # i_rdc_C, q_rdc_C, w_rdc_C, i_rdc_c, q_rdc_c, w_rdc_c = self.rdc_c_temp_calc(f_c)
        # i_rdc_B, q_rdc_B, w_rdc_B, i_rdc_b, q_rdc_b, w_rdc_b = self.rdc_b_temp_calc(f_b)
        # q_cdc, w_cdc, q_cdc_rdc, w_cdc_rdc = self.cdc_rdc_temp_calc(q_rdc_C, w_rdc_C, q_rdc_B, w_rdc_B)
        cdc_shipping_cost, cdc_shipping_cost_d, = self.cdc_shipping_cost(cdc_rdc_category)
        m = 0
        for cdc_name in cdc_cand:
            cdc_output['CDC'] = cdc_name
            cdc_output['shipping_cost'] = cdc_shipping_cost_d[cdc_name]
            cdc_output['Quantity'] = sum(cdc_rdc_category[cdc_name, rdc_name, category]
                                         for rdc_name in rdc_cand for category in category_list)
            cdc_output['Quantity_SKU1'] = sum(cdc_rdc_category[cdc_name, rdc_name, 'SKU1']
                                         for rdc_name in rdc_cand)
            cdc_output['Quantity_SKU2'] = sum(cdc_rdc_category[cdc_name, rdc_name, 'SKU2']
                                         for rdc_name in rdc_cand)
            cdc_output['Quantity_SKU3'] = sum(cdc_rdc_category[cdc_name, rdc_name, 'SKU3']
                                         for rdc_name in rdc_cand)
            cdc_output['Quantity_SKU4'] = sum(cdc_rdc_category[cdc_name, rdc_name, 'SKU4']
                                         for rdc_name in rdc_cand)
            cdc_output['Quantity_SKU5'] = sum(cdc_rdc_category[cdc_name, rdc_name, 'SKU5']
                                         for rdc_name in rdc_cand)
            # cdc_output['Quantity_SKU6'] = sum(cdc_rdc_category[cdc_name, rdc_name, 'SKU6']
            #                              for rdc_name in rdc_cand)
            df = df.append(pd.DataFrame(cdc_output, index=[m]))
            m = m+1

        return df, cdc_shipping_cost

    def rdc_post_process(self):
        """
        doing the post process of rdc
        :return:
        """
        rdc_output = {}
        df = pd.DataFrame()
        # rdc_cand = self.data_class.rdc_cand
        rdc_cost_loc = self.data_class.rdc_cost_loc
        customer = self.data_class.customer
        demand = self.data_class.demand
        city_add = self.data_class.city_add
        category_list = self.config.category_list
        category_info = self.data_class.category_info
        f_c = self.f_c

        rdc_open = self.rdc_open
        # # filter all items that cdc_open = 1, get the valid
        rdc_open_valid = {k:v for k,v in rdc_open.items() if v==1}

        # v_cdc_rdc, v_cdc = self.cdc_rdc_temp_calc(f_c)
        rdc_shipping_cost, rdc_shipping_cost_d, rdc_shipping_distance = self.rdc_shipping_cost(f_c)
        rdc_storage_cost, rdc_storage_cost_d = self.rdc_storage_cost(f_c)
        rdc_inbound, rdc_outbound, rdc_inbound_cost, rdc_outbound_cost = self.rdc_handling_cost(f_c)
        # rdc_capital_cost, rdc_capital_cost_d = self.capital_cost(i_rdc_c)

        m = 0
        for rdc_name in rdc_open_valid:
            rdc_output['RDC'] = rdc_name
            rdc_output['City_name'] = city_add[rdc_name]['city']
            rdc_output['shipping_cost'] = rdc_shipping_cost_d[rdc_name]
            rdc_output['storage_cost'] = rdc_storage_cost_d[rdc_name]
            rdc_output['Inbound_Handling_Cost'] = rdc_inbound_cost[rdc_name]
            rdc_output['Outbound_Handling_Cost'] = rdc_outbound_cost[rdc_name]
            # rdc_output['Reverse_Handling_Cost'] = rdc_r_outbound_cost[rdc_name]
            rdc_output['Inventory'] = sum(f_c[rdc_name, name] * demand[name][category]/YEAR_DAY
                                          * category_info[category]['turn_over_day']
                                          + category_info[category]['safety_inventory']
                                          for name in customer for category in category_list)
            rdc_output['Area'] = sum((f_c[rdc_name, name]*demand[name][category]/YEAR_DAY
                                          * category_info[category]['turn_over_day']
                                          + category_info[category]['safety_inventory']) * category_info[category]['area']
                                     *(1+self.config.area_ratio)
                                          for name in customer for category in category_list)
            # rdc_output['Capital_Cost'] = rdc_capital_cost_d[rdc_name]
            rdc_output['Quantity'] = sum(f_c[rdc_name, name] * demand[name]['demand_sum']
                                         for name in customer)
            rdc_output['Total_Cost'] = rdc_output['shipping_cost'] + rdc_output['storage_cost']
            if rdc_output['Quantity'] > 0:
                rdc_output['Price_avg'] = rdc_output['Total_Cost'] / rdc_output['Quantity']
                rdc_output['shipment_avg'] = rdc_output['shipping_cost'] / rdc_output['Quantity']
            # rdc_output['Volume'] = v_rdc[rdc_name]

            df = df.append(pd.DataFrame(rdc_output, index=[m]))
            m = m+1

        # TODO:
        # rdc_inbound = 0
        # rdc_outbound = 0
        # rdc_r_outbound = 0
        # rdc_capital_cost = 0
        return df, rdc_shipping_cost, rdc_storage_cost, rdc_inbound, rdc_outbound

    def c_end_network(self):
        """
        output the b end network
        customer, attributes of customer, cdc, attributes of cdc, service type,
        quantity, weight, inventory_quantity, sla, time
        :return:
        """
        # parameters
        customer = self.data_class.customer
        demand = self.data_class.demand
        distribution_price = self.data_class.distribution_price
        city_add = self.data_class.city_add
        category_list = self.config.category_list
        # sla = self.data_class.c_sla
        # define the network dict
        c_network = {}
        # get the cdc_open, f_c
        rdc_open = self.rdc_open
        # filter all items that cdc_open = 1, get the valid
        rdc_open_valid = {k: v for k, v in rdc_open.items() if v == 1}
        f_c = self.f_c
        category_info = self.data_class.category_info
        # loop
        k = 0
        df_c = pd.DataFrame()

        for rdc_name in rdc_open_valid.keys():
            c_network['RDC'] = rdc_name
            c_network['RDC_NAME'] = city_add[rdc_name]['city']
            c_network['RDC_LAT'] = city_add[rdc_name]['lat']
            c_network['RDC_LGT'] = city_add[rdc_name]['lgt']
            for c_name in customer:
                if f_c[rdc_name, c_name] >= 0.90:
                    c_network['CUSTOMER'] = c_name
                    c_network['QUANTITY'] = f_c[rdc_name, c_name] * demand[c_name]['demand_sum']

                    c_network['WEIGHT'] = f_c[rdc_name, c_name] * demand[c_name]['demand_weight_sum']
                    # c_network['SLA'] = f_c[rdc_name, c_name] * distribution_price[rdc_name, c_name]['sla_toC']
                    c_network['CUSTOMER_LGT'] = city_add[c_name]['lgt']
                    c_network['CUSTOMER_LAT'] = city_add[c_name]['lat']
                    c_network['CUSTOMER_NAME'] = city_add[c_name]['city']
                    # c_network['TIME'] = distribution_price[rdc_name, c_name]['time_median_toC']
                    df_c = df_c.append(pd.DataFrame(c_network, index=[k]))
                    k = k+1

        return df_c

    def cdc_rdc_network(self):
        """
        output the b end network
        customer, attributes of customer, cdc, attributes of cdc, service type,
        quantity, weight, inventory_quantity, sla, time
        :return:
        """
        # parameters
        customer = self.data_class.customer
        cdc_cand = self.data_class.cdc_cand
        demand = self.data_class.demand
        distribution_price = self.data_class.distribution_price
        city_add = self.data_class.city_add
        category_list = self.config.category_list
        # sla = self.data_class.c_sla
        # define the network dict
        cdc_rdc_network = {}
        # get the cdc_open, f_c
        rdc_open = self.rdc_open
        # filter all items that cdc_open = 1, get the valid
        rdc_open_valid = {k: v for k, v in rdc_open.items() if v > 0.5}
        cdc_rdc_category = self.cdc_rdc_category
        category_info = self.data_class.category_info
        # loop
        k = 0
        df_c = pd.DataFrame()
        for cdc_name in cdc_cand:
            for rdc_name in rdc_open_valid.keys():
                # cdc_rdc_network['CDC'] = cdc_name
                # cdc_rdc_network['CDC_NAME'] = city_add[cdc_name]['city_name']
                # cdc_rdc_network['CDC_LAT'] = city_add[cdc_name]['lat']
                # cdc_rdc_network['CDC_LGT'] = city_add[cdc_name]['lgt']
                # cdc_rdc_network['RDC'] = rdc_name
                # cdc_rdc_network['RDC_NAME'] = city_add[rdc_name]['city_name']
                # cdc_rdc_network['RDC_LAT'] = city_add[rdc_name]['lat']
                # cdc_rdc_network['RDC_LGT'] = city_add[rdc_name]['lgt']
                # cdc_rdc_network["WEIGHT"] = sum(cdc_rdc_category[cdc_name, rdc_name, category]
                #                                 for category in category_list)
                category_demand_list = []
                for category in category_list:
                    if cdc_rdc_category[cdc_name, rdc_name, category] >= 1:
                        category_demand_list.append(cdc_rdc_category[cdc_name, rdc_name, category])
                    else:
                        category_demand_list.append(0)
                if sum(category_demand_list)>0:
                    category_demand_list = [cdc_name, city_add[cdc_name]['lat'], city_add[cdc_name]['lgt'],
                                            rdc_name, city_add[rdc_name]['lat'], city_add[rdc_name]['lgt']]\
                                           + category_demand_list
                    df_c = df_c.append([category_demand_list])
                else:
                    continue
        df_c.columns = ['CDC_Name', 'CDC_LAT', 'CDC_LGT', 'RDC_Name','RDC_LAT', 'RDC_LGT', 'SKU1','SKU2','SKU3','SKU4','SKU5']
        return df_c
    # def b_end_network(self):
    #     """
    #     output the b end network
    #     customer, attributes of customer, cdc, attributes of cdc, service type,
    #     quantity, weight, inventory_quantity, sla, time
    #     :return:
    #     """
    #     # parameters
    #     customer = self.data_class.customer_2b
    #     demand = self.data_class.demand_2b
    #     distribution_price = self.data_class.distribution_price
    #     city_add = self.data_class.city_add
    #     # sla = self.data_class.c_sla
    #     # define the network dict
    #     c_network = {}
    #     # get the cdc_open, f_c
    #     rdc_open = self.rdc_open
    #     # filter all items that cdc_open = 1, get the valid
    #     rdc_open_valid = {k: v for k, v in rdc_open.items() if v == 1}
    #     cdc_rdc_category = self.cdc_rdc_category
    #
    #     # loop
    #     k = 0
    #     df_b = pd.DataFrame()
    #
    #     for rdc_name in rdc_open_valid.keys():
    #         c_network['RDC'] = rdc_name
    #         c_network['RDC_NAME'] = city_add[rdc_name]['city_name']
    #         c_network['RDC_LAT'] = city_add[rdc_name]['lat']
    #         c_network['RDC_LGT'] = city_add[rdc_name]['lgt']
    #         for c_name in customer:
    #             if f_b[rdc_name, c_name] >= 0.90:
    #                 c_network['CUSTOMER'] = c_name
    #                 c_network['QUANTITY'] = f_b[rdc_name, c_name] * demand[c_name]['demand_sum']
    #                 c_network['WEIGHT'] = f_b[rdc_name, c_name] * demand[c_name]['demand_weight_sum']
    #                 c_network['SLA'] = f_b[rdc_name, c_name] * distribution_price[rdc_name, c_name]['sla_toB']
    #                 c_network['CUSTOMER_LGT'] = city_add[c_name]['lgt']
    #                 c_network['CUSTOMER_LAT'] = city_add[c_name]['lat']
    #                 c_network['CUSTOMER_NAME'] = city_add[c_name]['city_name']
    #                 c_network['TIME'] = distribution_price[rdc_name, c_name]['time_median_toB']
    #                 df_b = df_b.append(pd.DataFrame(c_network, index=[k]))
    #                 k = k+1
    #         else:
    #             continue
    #     # log.info('the df_c: \n {}'.format(df_c))
    #     return df_b




