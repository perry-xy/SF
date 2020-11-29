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
# define the log file
log = Logger(log_path='../log').logger

# define the facility location problem


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
        self.sla = None
        self.f_b = None
        self.sla_b = None
        self.cdc_open = None
        self.demand_rdc = None

    def facility_location(self):
        """
        the function will build a facility location optimization model which take the self.data_class input as parameters.
        return the optimized model
        :return: model
        """
        """######## list the parameters ######### """
        # the current cdc and must use cdc
        cdc_current = self.data_class.cdc_current
        # the current rdc and must use rdc
        rdc_current = self.data_class.rdc_current
        rdc_use = self.data_class.rdc_use
        # candidate list of potential locations of CDC, RDC and customer list
        customer = self.data_class.customer
        customer_2b = self.data_class.customer_2b
        cdc_cand = self.data_class.cdc_cand
        rdc_cand = self.data_class.rdc_cand
        # parameters
        trunk_price = self.data_class.trunk_price
        rdc_capacity = self.data_class.rdc_capacity
        distribution_price = self.data_class.distribution_price
        demand = self.data_class.demand
        demand_2b = self.data_class.demand_2b
        num_rdc = self.config.num_rdc
        """######## declare the model ######### """
        model = Model('facility_location_Apple')

        """######## define the decision variables ######### """
        # the indicator of open status of RDC
        cdc_open = model.addVars(cdc_cand, vtype=GRB.BINARY, name='cdc_open')
        # the indicator of open status of RDC
        rdc_open = model.addVars(rdc_cand, vtype=GRB.BINARY, name='rdc_open')
        # fraction of demand of RDC supplied by OEM
        demand_rdc = model.addVars(cdc_cand, rdc_cand, vtype=GRB.INTEGER, name='demand_rdc')
        # fraction of demand of customer supplied by RDC
        f_b = model.addVars(rdc_cand, customer_2b, vtype=GRB.BINARY, name='f_b')
        # fraction of demand of customer supplied by RDC
        f_c = model.addVars(rdc_cand, customer, vtype=GRB.BINARY, name='f_c')
        # the indicator of reachable of SLA
        sla_b = model.addVars(customer_2b, vtype=GRB.BINARY, name='sla_b')
        sla = model.addVars(customer, vtype=GRB.BINARY, name='sla_c')
        """######## define the constrains ######### """
        # the current of cdc and rdc should be used constraints
        if self.config.current_location_constr_open:
            model.addConstrs((cdc_open[cdc_name] == 1 for cdc_name in cdc_current), name='cdc_current_constr')
            model.addConstrs((rdc_open[rdc_name] == 1 for rdc_name in rdc_current), name='rdc_current_constr')
        if self.config.use_location_constr_open:
            # rdc_use = ['20', '22', '23', '28', '29', '351', '378', '411', '431', '451', '471','539','574', '594', '714', '7311',
            #            '772','791', '851', '871', '991']
            rdc_use = ['512']
            model.addConstrs((rdc_open[rdc_name] == 1 for rdc_name in rdc_use), name='rdc_use_constr')
        # the number of potential location of RDC constraints
        model.addConstr(sum(rdc_open[rdc_name] for rdc_name in rdc_cand) == num_rdc, name='num_rdc')
        # the demand of  customer should be met constrains
        model.addConstrs((f_b.sum('*', name) == 1 for name in customer_2b), name='2b_demand')
        model.addConstrs((f_c.sum('*', name) == 1 for name in customer), name='demand')

        # the demand of each customer the rdc should be met

        for rdc_name in rdc_cand:
            model.addConstrs((f_c[rdc_name, name] <= rdc_open[rdc_name] for name in customer), name='f_c_cons')
            model.addConstrs((f_b[rdc_name, name] <= rdc_open[rdc_name] for name in customer_2b), name='f_b_cons')
        # the capacity constrains
        rr = self.config.rr
        total_days = 365
        if self.config.capacity_constrs_open:
            for rdc_name in rdc_cand:
                model.addConstr((sum((f_c[rdc_name, name] * demand[name]['demand_sum']
                                     for name in customer)) +
                                sum((f_c[rdc_name, name] * demand[name]['demand_sum'] for name in
                                     customer)) * rr * self.config.return_turnover_days
                            + sum((f_b[rdc_name, name] * demand_2b[name]['demand_sum']
                                   for name in customer_2b)))/total_days
                                <= rdc_open[rdc_name] * rdc_capacity[rdc_name], name='capacity_constr')
            # model.addConstr(sum((f_c[rdc_name, name] * demand[name]['demand_sum'] * self.config.turnover_days_c
            #                      for name in customer)) +
            #                 sum((f_c[rdc_name, name] * demand[name]['demand_sum'] for name in
            #                      customer)) * rr * self.config.return_turnover_days
            #             + sum((f_b[rdc_name, name] * demand_2b[name]['demand_sum'] * self.config.turnover_days_b
            #                    for name in customer_2b))
            #                 >= rdc_capacity[rdc_name], name='capacity_constr_1')

        # the toC sla constrains
        SLA_NUM = 100
        if self.config.sla_2c_constrs_open:

            for name in customer:
                model.addConstr(sum(f_c[rdc_name, name] * distribution_price[rdc_name, name][self.config.time_quantile_c]
                                    for rdc_name in rdc_cand)
                    - sum(f_c[rdc_name, name] * distribution_price[rdc_name, name]['sla_toC']
                                    for rdc_name in rdc_cand) <= SLA_NUM * (1 - sla[name]), name='sla_constr')
                model.addConstr(sum(f_c[rdc_name, name] * distribution_price[rdc_name, name][self.config.time_quantile_c]
                                    for rdc_name in rdc_cand)
                     - sum(f_c[rdc_name, name] * distribution_price[rdc_name, name]['sla_toC']
                                    for rdc_name in rdc_cand) >= -SLA_NUM * sla[name], name='sla_constr_1')
            # model.addConstr(sum(sla[name] * demand[name]['demand_sum'] for name in customer) >=
            #             self.config.P_c * sum(demand[name]['demand_sum'] for name in customer),
            #             name='sla_constr_2')
        # toB sla  constrains
        if self.config.sla_2b_constrs_open:
            for name in customer_2b:
                model.addConstr(sum(f_b[rdc_name, name] * distribution_price[rdc_name, name][self.config.time_quantile_b]
                                    for rdc_name in rdc_cand)
                    - sum(f_b[rdc_name, name] * distribution_price[rdc_name, name]['sla_toB']
                                    for rdc_name in rdc_cand) <= SLA_NUM * (1 - sla_b[name]), name='sla_constr_2b')
                model.addConstr(sum(f_b[rdc_name, name] * distribution_price[rdc_name, name][self.config.time_quantile_b]
                                    for rdc_name in rdc_cand)
                    - - sum(f_b[rdc_name, name] * distribution_price[rdc_name, name]['sla_toB']
                                    for rdc_name in rdc_cand) >= -SLA_NUM * sla_b[name], name='sla_constr_2b_1')
            model.addConstr(sum(sla_b[name] * demand_2b[name]['demand_sum'] for name in customer_2b) >=
                        self.config.P_b * sum(demand_2b[name]['demand_sum'] for name in customer_2b),
                        name='sla_constr_2b_2')


        """######## define the objective function ######### """
        # define the shipping cost of cdc
        # intermediate variables
        # v_cdc_rdc, v_cdc = self.cdc_rdc_temp_calc(f_c, f_b)
        i_rdc_C, q_rdc_C, w_rdc_C, i_rdc_c, q_rdc_c, w_rdc_c, q_rdc_sum = self.rdc_c_temp_calc(f_c)
        i_rdc_B, q_rdc_B, w_rdc_B, i_rdc_b, q_rdc_b, w_rdc_b = self.rdc_b_temp_calc(f_b)

        for cdc_name in cdc_cand:
            for rdc_name in rdc_cand:
                model.addConstr(demand_rdc[cdc_name, rdc_name] >= rdc_open[rdc_name], name='cdc_demand_constr')
        # allocation the quantity which the CDC must to supply
        # TODO: can set the low quantity and high quantity of oem must supply
        # low_quantity = self.config.low_quantity
        # high_quantity = self.config.high_quantity
        low_quantity = 50000
        for cdc_name in cdc_cand:
            model.addConstr(demand_rdc.sum(cdc_name, '*') >= cdc_open[cdc_name] * low_quantity, name='cdc_demand_constr')
            model.addConstr(demand_rdc.sum(cdc_name, '*') <= cdc_open[cdc_name] * rdc_capacity[cdc_name],
                            name='cdc_demand_constr')
        for rdc_name in rdc_cand:
            model.addConstr(demand_rdc.sum('*', rdc_name) - q_rdc_sum[rdc_name] - q_rdc_B[rdc_name] >= 0, name='demand_cons')
            model.addConstr(demand_rdc.sum('*', rdc_name) - q_rdc_sum[rdc_name] - q_rdc_B[rdc_name] <= 1, name='demand_cons')
        q_cdc, q_cdc_rdc = self.cdc_rdc_temp_calc_use(demand_rdc)
        cdc_shipping_cost, cdc_shipping_cost_r, cdc_shipping_cost_d, cdc_shipping_cost_r_d =\
            self.cdc_shipping_cost(demand_rdc)
        rdc_shipping_cost, rdc_shipping_cost_d, shipping_cost_d_b, shipping_cost_d_c, \
        shipping_cost_b, shipping_cost_c = self.rdc_shipping_cost(q_rdc_c, w_rdc_c, w_rdc_b, f_b)
        # rdc_storage_cost, rdc_storage_cost_d = self.rdc_storage_cost(i_rdc_c)
        # rdc_inbound, rdc_outbound, rdc_r_outbound, rdc_inbound_cost, rdc_outbound_cost, rdc_r_outbound_cost =\
        #     self.rdc_handling_cost(q_rdc_c)
        # rdc_capital_cost, rdc_capital_cost_d = self.capital_cost(i_rdc_c)
        # cost = cdc_shipping_cost + cdc_shipping_cost_r + rdc_shipping_cost + rdc_storage_cost\
        #        + rdc_inbound + rdc_outbound + rdc_r_outbound + rdc_capital_cost
        cost_penalty = 1.0
        time_penalty = 0.1
        sla_penalty_c = 10
        sla_penalty_b = 0.0
        time_used = self.rdc_time_calc(f_c, f_b)
        sla_c_obj = sum(sla[name] * demand[name]['demand_sum'] for name in customer) - \
                  self.config.P_c * sum(demand[name]['demand_sum'] for name in customer)
        sla_b_obj = sum(sla_b[name] * demand_2b[name]['demand_sum'] for name in customer_2b) - \
                  self.config.P_b * sum(demand_2b[name]['demand_sum'] for name in customer_2b)
        cost = cost_penalty * (shipping_cost_c ) + time_penalty * time_used \
               - sla_penalty_c * sla_c_obj - sla_penalty_b * sla_b_obj
        # set the Objectice function
        model.setObjective(cost, GRB.MINIMIZE)
        # set the MIXGAP
        # model.setParam('MIPGap', 1e-3)
        """######## solve the model ######### """
        model.optimize()

        """######## check the model ######### """
        if model.Status == GRB.OPTIMAL:
            log.info('the facility location optimized sucessfully !')
            log.info('the objective of the model is {}'.format(model.objVal))
            self.model = model
            log.info('dump model to file:\n')
            model.write('facility location ft.rlp')

            # get all solved variables
            self.cdc_open = model.getAttr('x', cdc_open)
            self.rdc_open = model.getAttr('x', rdc_open)
            self.f_c = model.getAttr('x', f_c)
            self.f_b = model.getAttr('x', f_b)
            self.sla_b = model.getAttr('x', sla_b)
            self.sla = model.getAttr('x', sla)
            self.demand_rdc = model.getAttr('x', demand_rdc)
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

    def cdc_shipping_cost(self, demand_rdc):
        """
        calculate the shipping cost from cdc to rdc and
        reverse shipping cost from rdc to cdc
        the equation of shipping cost is following :
        tl_v_cdc_rdc = v_cdc_rdc * 30%
        ftl_v_cdc_rdc = v_cdc_rdc * 70% / self.config.volume_per_truck


        :param: v_cdc_rdc the volume from cdc to rdc
        :return:
            shipping_cost: shipping cost from cdc to rdc
            shipping_cost_r: reverse shipping cost from rdc to cdc
            shipping_cost_d: shipping cost from cdc to rdc for each cdc
            shipping_cost_r_d: reverse shipping cost from rdc to cdc for each cdc
        """

        # parameters
        rdc_cand = self.data_class.rdc_cand
        rdc_cost_loc = self.data_class.rdc_cost_loc
        trunk_price = self.data_class.trunk_price
        cdc_rand = self.data_class.cdc_cand
        weight_avg = self.config.weight_avg
        q_cdc_rdc = demand_rdc
        shipping_cost = 0
        shipping_cost_r = 0
        shipping_cost_d = {}
        shipping_cost_r_d = {}
        for cdc_name in cdc_rand:
            ship_c = 0
            for rdc_name in rdc_cand:
                # 小票零担
                tl_w_cdc_rdc = weight_avg * q_cdc_rdc[cdc_name, rdc_name] * self.config.ltr / rdc_cost_loc[rdc_name]['replenishment']
                ship_tl_tmp = (tl_w_cdc_rdc-trunk_price[cdc_name, rdc_name]['base_weight_qty_tl']) \
                             * trunk_price[cdc_name, rdc_name]['weight_price_qty_tl'] \
                              + trunk_price[cdc_name, rdc_name]['base_price_tl']

                # 重货专运
                ftl_w_cdc_rdc = weight_avg * q_cdc_rdc[cdc_name, rdc_name] * (1 - self.config.ltr) / rdc_cost_loc[rdc_name]['replenishment']
                ship_ftl_tmp = (ftl_w_cdc_rdc - trunk_price[cdc_name, rdc_name]['base_weight_qty_ftl']) * \
                               trunk_price[cdc_name, rdc_name]['weight_price_qty_ftl'] \
                              + trunk_price[cdc_name, rdc_name]['base_price_ftl']

                ship_c += (ship_tl_tmp + ship_ftl_tmp) * rdc_cost_loc[rdc_name]['replenishment']

            shipping_cost_d[cdc_name] = ship_c
            shipping_cost_r_d[cdc_name] = ship_c * self.config.rr_cdc
            shipping_cost += ship_c
        shipping_cost_r = shipping_cost * self.config.rr_cdc

        return shipping_cost, shipping_cost_r, shipping_cost_d, shipping_cost_r_d

    # TODO: shipping cost by order from rdc to customer
    def rdc_shipping_cost(self, d_rdc_c, w_rdc_C, w_rdc_B, f_b):
        """
        calculate the shipping cost from rdc to customer
        reverse from c to rdc is omit now
        :param: d_rdc_c: the quantity of demand from customer
        :return:
        """
        # parameters
        upo_dist = self.config.upo_dist
        rdc_cand = self.data_class.rdc_cand
        customer = self.data_class.customer
        customer_2b = self.data_class.customer_2b
        demand_2b = self.data_class.demand_2b
        distribution_price = self.data_class.distribution_price

        # declare variables
        shipping_cost_d = {}
        shipping_cost_d_b = {}
        shipping_cost_d_c = {}
        shipping_cost_c = 0
        shipping_cost_b = 0

        for rdc_name in rdc_cand:
            ship_more_toC = 0
            ship_less_toC = 0
            for name in customer:
                ship_more_toC += d_rdc_c[rdc_name, name]['more'] / upo_dist * \
                             ((w_rdc_C[rdc_name, name] - distribution_price[rdc_name, name]['base_weight_qty_toC']) *
                              distribution_price[rdc_name, name]['weight_price_qty_toC']
                              + distribution_price[rdc_name, name]['base_price_toC'])
                # less shipping cost
                ship_less_toC += d_rdc_c[rdc_name, name]['less'] / upo_dist * \
                             distribution_price[rdc_name, name]['base_price_toC']

            shipping_cost_c += ship_more_toC + ship_less_toC
            # TODO: 首重和续重这个计算法方式 在零的时候 会导致 多计算了 首重费用
            # toB customer
            ship_toB = 0
            for name in customer_2b:
                w_rdc_tmp = w_rdc_B[rdc_name, name] / demand_2b[name]['replenishment']
                ship_toB += f_b[rdc_name, name] * ((w_rdc_tmp - distribution_price[rdc_name, name]['base_weight_qty_toB']) *
                              distribution_price[rdc_name, name]['weight_price_qty_toB']
                              + distribution_price[rdc_name, name]['base_price_toB']) * demand_2b[name]['replenishment']

            shipping_cost_b += ship_toB
            # each RDC
            shipping_cost_d[rdc_name] = ship_toB + ship_less_toC + ship_more_toC
            shipping_cost_d_b[rdc_name] = ship_toB
            shipping_cost_d_c[rdc_name] = ship_less_toC + ship_more_toC
            # all RDC
        shipping_cost = shipping_cost_b + shipping_cost_c

        return shipping_cost, shipping_cost_d, shipping_cost_d_b, shipping_cost_d_c, shipping_cost_b, shipping_cost_c

    def rdc_storage_cost(self, i_rdc_c, i_rdc_b):
        """
        calculate the storage cost of rdc

        :param: I_rdc_c: the average inventory from rdc to c
        :return:
        """
        # parameters
        rdc_cand = self.data_class.rdc_cand
        customer = self.data_class.customer
        customer_2b = self.data_class.customer_2b
        rdc_cost_loc = self.data_class.rdc_cost_loc
        # declare variables
        rdc_storage_cost = {}
        rdc_storage_cost_c = {}
        rdc_storage_cost_b = {}
        rdc_storage_c = 0
        rdc_storage_b = 0

        for rdc_name in rdc_cand:
            rdc_storage_c_sum = 0
            rdc_storage_b_sum = 0
            for name in customer:
                rdc_storage_tmp = i_rdc_c[rdc_name, name]/rdc_cost_loc[rdc_name]['unit_area'] \
                                  * 12 * rdc_cost_loc[rdc_name]['monthly_rental_price']
                rdc_storage_c_sum += rdc_storage_tmp
            rdc_storage_c += rdc_storage_c_sum
            rdc_storage_cost_c[rdc_name] = rdc_storage_c_sum

            for name in customer_2b:
                rdc_storage_tmp_b = i_rdc_b[rdc_name, name]/rdc_cost_loc[rdc_name]['unit_area'] \
                                  * 12 * rdc_cost_loc[rdc_name]['monthly_rental_price']
                rdc_storage_b_sum += rdc_storage_tmp_b
            rdc_storage_b += rdc_storage_b_sum
            rdc_storage_cost_b[rdc_name] = rdc_storage_b_sum
            rdc_storage_cost[rdc_name] = rdc_storage_b_sum + rdc_storage_c_sum
        rdc_storage = rdc_storage_c + rdc_storage_b
        return rdc_storage, rdc_storage_cost

    def reverse_cost(self):
        """
        reverse cost is consisted of reverse shipping cost, reverse inbound & outbound cost, reverse inventory cost
        :return:
        """

        reverse_shipping_cost = 0
        reverse_inbound_cost = 0
        reverse_outbound_cost = 0
        reverse_storage_cost = 0
        return reverse_shipping_cost, reverse_inbound_cost, reverse_outbound_cost, reverse_storage_cost

    def rdc_handling_cost(self, q_rdc_c):
        """
        calculate the handling cost
            inbound cost:
            outbound cost
        :return:
        """
        # parameters
        rdc_cand = self.data_class.rdc_cand
        rdc_cost_loc = self.data_class.rdc_cost_loc
        upo = self.config.upo
        # declare
        rdc_inbound = 0
        rdc_outbound = 0
        rdc_r_outbound = 0
        rdc_inbound_cost = {}
        rdc_outbound_cost = {}
        rdc_r_outbound_cost = {}

        for rdc_name in rdc_cand:
            rdc_inbound_tmp = (q_rdc_c[rdc_name]['sum']) * rdc_cost_loc[rdc_name]['inbound_piece_price']

            rdc_outbound_tmp = (q_rdc_c[rdc_name]['cooker_hood'] + q_rdc_c[rdc_name]['others'])/upo \
                               * rdc_cost_loc[rdc_name]['outbound_order_price']
            rdc_inbound += rdc_inbound_tmp
            rdc_outbound += rdc_outbound_tmp
            rdc_r_outbound += rdc_outbound_tmp * self.config.rr_cdc

            rdc_inbound_cost[rdc_name] = rdc_inbound_tmp
            rdc_outbound_cost[rdc_name] = rdc_outbound_tmp
            rdc_r_outbound_cost[rdc_name] = rdc_outbound_tmp * self.config.rr_cdc

        return rdc_inbound, rdc_outbound, rdc_r_outbound, rdc_inbound_cost, rdc_outbound_cost, rdc_r_outbound_cost

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

    # the intermediate variables should include v_cdc_rdc, w_rdc_c, i_rdc_c, q_rdc_c
    # TODO: define
    def cdc_rdc_temp_calc_use(self, demand_rdc):
        """
        calculate the intermediate variables from cdc to rdc
        :return:
            v_cdc_rdc： the volume of yearly demand of all from cdc to rdc
        """

        q_cdc = {}

        # parameters

        rdc_cand = self.data_class.rdc_cand
        cdc_cand = self.data_class.cdc_cand
        q_cdc_rdc = demand_rdc

        for cdc_name in cdc_cand:
            q_cdc_tmp = 0
            for rdc_name in rdc_cand:
                q_cdc_tmp += demand_rdc[cdc_name, rdc_name]

            q_cdc[cdc_name] = q_cdc_tmp

        return q_cdc, q_cdc_rdc

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
                q_cdc_tmp += q_rdc_C[rdc_name] + q_rdc_B[rdc_name]
                w_cdc_tmp += w_rdc_C[rdc_name] + w_rdc_B[rdc_name]

                q_cdc_rdc[cdc_name, rdc_name] = q_rdc_C[rdc_name] + q_rdc_B[rdc_name]
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
        q_rdc_sum = {}
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
            q_rdc_sum[rdc_name] = q_rdc_less + q_rdc_more
            w_rdc[rdc_name] = w_rdc_less + w_rdc_more

        return i_rdc, q_rdc, w_rdc, i_rdc_c, q_rdc_c, w_rdc_c,q_rdc_sum

        # post-process of cdc

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
               time_used_temp += f_c[rdc_name, name] * distribution_price[name, rdc_name][self.config.time_quantile_c]
            for name_b in customer_2b:
                time_used_temp += f_b[rdc_name, name_b] * distribution_price[name_b, rdc_name][self.config.time_quantile_b]
            time_used += time_used_temp
        return time_used

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
        demand_rdc = self.demand_rdc
        q_cdc, q_cdc_rdc = self.cdc_rdc_temp_calc_use(demand_rdc)
        cdc_shipping_cost, cdc_shipping_cost_r, cdc_shipping_cost_d, cdc_shipping_cost_r_d =\
            self.cdc_shipping_cost(demand_rdc)
        m = 0
        for cdc_name in cdc_cand:
            cdc_output['CDC'] = cdc_name
            cdc_output['shipping_cost'] = cdc_shipping_cost_d[cdc_name] + cdc_shipping_cost_r_d[cdc_name]
            cdc_output['quantity'] = q_cdc[cdc_name]
            df = df.append(pd.DataFrame(cdc_output, index=[m]))
            m = m+1

        return df, cdc_shipping_cost, cdc_shipping_cost_r

    def rdc_post_process(self):
        """
        doing the post process of rdc
        :return:
        """
        rdc_output = {}
        df = pd.DataFrame()
        # rdc_cand = self.data_class.rdc_cand
        rdc_cost_loc = self.data_class.rdc_cost_loc
        city_add = self.data_class.city_add
        f_c = self.f_c
        f_b = self.f_b
        demand_rdc = self.demand_rdc
        rdc_open = self.rdc_open
        cdc_open = self.cdc_open
        # # filter all items that cdc_open = 1, get the valid
        rdc_open_valid = {k:v for k,v in rdc_open.items() if v==1}
        cdc_open_valid = {k: v for k, v in cdc_open.items() if v == 1}

        # v_cdc_rdc, v_cdc = self.cdc_rdc_temp_calc(f_c)
        i_rdc_C, q_rdc_C, w_rdc_C, i_rdc_c, q_rdc_c, w_rdc_c,q_rdc_sum = self.rdc_c_temp_calc(f_c)
        i_rdc_B, q_rdc_B, w_rdc_B, i_rdc_b, q_rdc_b, w_rdc_b = self.rdc_b_temp_calc(f_b)
        rdc_shipping_cost, rdc_shipping_cost_d, shipping_cost_d_b, shipping_cost_d_c, shipping_cost_b, shipping_cost_c\
            = self.rdc_shipping_cost(q_rdc_c, w_rdc_c, w_rdc_b, f_b)
        rdc_storage_cost, rdc_storage_cost_d = self.rdc_storage_cost(i_rdc_c, i_rdc_b)
        # rdc_inbound, rdc_outbound, rdc_r_outbound, rdc_inbound_cost, rdc_outbound_cost, rdc_r_outbound_cost =\
        #     self.rdc_handling_cost(q_rdc_c)
        # rdc_capital_cost, rdc_capital_cost_d = self.capital_cost(i_rdc_c)

        m = 0

        #     rdc_output['CDC'] = cdc_name
        #     rdc_output['CDC_NAME'] = city_add[cdc_name]['city_name']
        #     rdc_output['CDC_LAT'] = city_add[cdc_name]['lat']
        #     rdc_output['CDC_LGT'] = city_add[cdc_name]['lgt']
        for rdc_name in rdc_open_valid:
            rdc_output['RDC'] = rdc_name
            rdc_output['City_name'] = city_add[rdc_name]['city_name']
            rdc_output['shipping_cost'] = rdc_shipping_cost_d[rdc_name]
            rdc_output['shipping_cost_toC'] = shipping_cost_d_c[rdc_name]
            rdc_output['shipping_cost_toB'] = shipping_cost_d_b[rdc_name]
            rdc_output['storage_cost'] = rdc_storage_cost_d[rdc_name]
            # rdc_output['Inbound_Handling_Cost'] = rdc_inbound_cost[rdc_name]
            # rdc_output['Outbound_Handling_Cost'] = rdc_outbound_cost[rdc_name]
            # rdc_output['Reverse_Handling_Cost'] = rdc_r_outbound_cost[rdc_name]
            demand_cdc = 0
            for cdc_name in cdc_open_valid.keys():
                demand_cdc += demand_rdc[cdc_name, rdc_name]
            rdc_output['QUANTITY_FROM_CDC'] = demand_cdc
            rdc_output['Inventory'] = i_rdc_C[rdc_name] + i_rdc_B[rdc_name]
            rdc_output['Area'] = rdc_output['Inventory'] / rdc_cost_loc[rdc_name]['unit_area']
            rdc_output['Quantity'] = q_rdc_sum[rdc_name] + q_rdc_B[rdc_name]
            rdc_output['Quantity_toC'] = q_rdc_sum[rdc_name]
            rdc_output['Quantity_toB'] = q_rdc_B[rdc_name]
            rdc_output['Total_Cost'] = rdc_output['shipping_cost'] + rdc_output['storage_cost']
            if rdc_output['Quantity']>0:
                rdc_output['Price_avg'] = rdc_output['Total_Cost']/rdc_output['Quantity']
                rdc_output['shipment_avg'] = rdc_output['shipping_cost']/rdc_output['Quantity']
                rdc_output['shipment_avg_toC'] = rdc_output['shipping_cost_toC']/rdc_output['Quantity_toC']
                rdc_output['shipment_avg_toB'] = rdc_output['shipping_cost_toB']/rdc_output['Quantity_toB']
            # rdc_output['Capital_Cost'] = rdc_capital_cost_d[rdc_name]
            # rdc_output['Volume'] = v_rdc[rdc_name]

            df = df.append(pd.DataFrame(rdc_output, index=[m]))
            m = m+1

            # TODO:
        rdc_inbound = 0
        rdc_outbound = 0
        rdc_r_outbound = 0
        rdc_capital_cost = 0
        return df, rdc_shipping_cost, shipping_cost_b, shipping_cost_c, \
               rdc_storage_cost, rdc_inbound, rdc_outbound, rdc_r_outbound, rdc_capital_cost

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
        # sla = self.data_class.c_sla
        # define the network dict
        c_network = {}
        # get the cdc_open, f_c
        rdc_open = self.rdc_open

        # filter all items that cdc_open = 1, get the valid
        rdc_open_valid = {k: v for k, v in rdc_open.items() if v == 1}

        f_c = self.f_c


        # loop
        k = 0
        df_c = pd.DataFrame()

        for rdc_name in rdc_open_valid.keys():
            c_network['RDC'] = rdc_name
            c_network['RDC_NAME'] = city_add[rdc_name]['city_name']
            c_network['RDC_LAT'] = city_add[rdc_name]['lat']
            c_network['RDC_LGT'] = city_add[rdc_name]['lgt']
            for c_name in customer:
                if f_c[rdc_name, c_name] >= 0.90:
                    c_network['CUSTOMER'] = c_name
                    c_network['QUANTITY'] = f_c[rdc_name, c_name] * demand[c_name]['demand_sum']
                    c_network['WEIGHT'] = f_c[rdc_name, c_name] * demand[c_name]['demand_weight_sum']
                    c_network['SLA'] = f_c[rdc_name, c_name] * distribution_price[rdc_name, c_name]['sla_toC']
                    c_network['CUSTOMER_LGT'] = city_add[c_name]['lgt']
                    c_network['CUSTOMER_LAT'] = city_add[c_name]['lat']
                    c_network['CUSTOMER_NAME'] = city_add[c_name]['city_name']
                    c_network['TIME'] = distribution_price[rdc_name, c_name][self.config.time_quantile_c]
                    c_network['INVERSE_TIME'] = distribution_price[c_name, rdc_name][self.config.time_quantile_c]
                    df_c = df_c.append(pd.DataFrame(c_network, index=[k]))
                    k = k+1
            else:
                continue
        # log.info('the df_c: \n {}'.format(df_c))
        return df_c

    def b_end_network(self):
        """
        output the b end network
        customer, attributes of customer, cdc, attributes of cdc, service type,
        quantity, weight, inventory_quantity, sla, time
        :return:
        """
        # parameters
        customer = self.data_class.customer_2b
        demand = self.data_class.demand_2b
        distribution_price = self.data_class.distribution_price
        city_add = self.data_class.city_add
        # sla = self.data_class.c_sla
        # define the network dict
        c_network = {}
        # get the cdc_open, f_c
        rdc_open = self.rdc_open
        # filter all items that cdc_open = 1, get the valid
        rdc_open_valid = {k: v for k, v in rdc_open.items() if v == 1}
        f_b = self.f_b

        # loop
        k = 0
        df_b = pd.DataFrame()

        for rdc_name in rdc_open_valid.keys():
            c_network['RDC'] = rdc_name
            c_network['RDC_NAME'] = city_add[rdc_name]['city_name']
            c_network['RDC_LAT'] = city_add[rdc_name]['lat']
            c_network['RDC_LGT'] = city_add[rdc_name]['lgt']
            for c_name in customer:
                if f_b[rdc_name, c_name] >= 0.90:
                    c_network['CUSTOMER'] = c_name
                    c_network['QUANTITY'] = f_b[rdc_name, c_name] * demand[c_name]['demand_sum']
                    c_network['WEIGHT'] = f_b[rdc_name, c_name] * demand[c_name]['demand_weight_sum']
                    c_network['SLA'] = f_b[rdc_name, c_name] * distribution_price[rdc_name, c_name]['sla_toB']
                    c_network['CUSTOMER_LGT'] = city_add[c_name]['lgt']
                    c_network['CUSTOMER_LAT'] = city_add[c_name]['lat']
                    c_network['CUSTOMER_NAME'] = city_add[c_name]['city_name']
                    c_network['TIME'] = distribution_price[rdc_name, c_name][self.config.time_quantile_b]
                    c_network['INVERSE_TIME'] = distribution_price[c_name, rdc_name][self.config.time_quantile_b]
                    df_b = df_b.append(pd.DataFrame(c_network, index=[k]))
                    k = k+1
            else:
                continue
        # log.info('the df_c: \n {}'.format(df_c))
        return df_b

    def cdc_rdc_network(self):
        """

        :return:
        """
        demand_rdc = self.demand_rdc
        cdc_cand = self.data_class.cdc_cand
        rdc_cand = self.data_class.rdc_cand
        city_add = self.data_class.city_add

        k = 0
        df_b = pd.DataFrame()
        cdc_rdc_network = {}
        for cdc_name in cdc_cand:
            cdc_rdc_network['CDC'] = cdc_name
            cdc_rdc_network['CDC_NAME'] = city_add[cdc_name]['city_name']
            cdc_rdc_network['CDC_LAT'] = city_add[cdc_name]['lat']
            cdc_rdc_network['CDC_LGT'] = city_add[cdc_name]['lgt']
            for rdc_name in rdc_cand:
                cdc_rdc_network['RDC_NAME'] = city_add[rdc_name]['city_name']
                cdc_rdc_network['RDC_LAT'] = city_add[rdc_name]['lat']
                cdc_rdc_network['RDC_LGT'] = city_add[rdc_name]['lgt']
                cdc_rdc_network['QUANTITY'] = demand_rdc[cdc_name, rdc_name]

                df_b = df_b.append(pd.DataFrame(cdc_rdc_network, index=[k]))
                k = k + 1
            return df_b


