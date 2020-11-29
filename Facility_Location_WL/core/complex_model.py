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
        distribution_price = self.data_class.distribution_price
        demand = self.data_class.demand
        demand_2b = self.data_class.demand_2b
        num_rdc = self.config.num_rdc
        num_cdc = self.config.num_cdc
        """######## declare the model ######### """
        model = Model('facility_location_Apple')

        """######## define the decision variables ######### """
        # the indicator of open status of RDC
        cdc_open = model.addVars(cdc_cand, vtype=GRB.BINARY, name='rdc_open')
        # the indicator of open status of RDC
        rdc_open = model.addVars(rdc_cand, vtype=GRB.BINARY, name='rdc_open')
        # fraction of demand of customer supplied by RDC
        f_b = model.addVars(cdc_cand, rdc_cand, customer_2b, vtype=GRB.CONTINUOUS, name='f_b')
        # fraction of demand of customer supplied by RDC
        f_c = model.addVars(cdc_cand, rdc_cand, customer, vtype=GRB.CONTINUOUS, name='f_c')
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
            # rdc_use = ['574']
            model.addConstrs((rdc_open[rdc_name] == 1 for rdc_name in rdc_use), name='rdc_use_constr')
        # the number of potential location of RDC constraints
        model.addConstr(sum(rdc_open[rdc_name] for rdc_name in rdc_cand) == num_rdc, name='num_rdc')
        model.addConstr(sum(cdc_open[cdc_name] for cdc_name in cdc_cand) == num_cdc, name='num_cdc')
        # the demand of  customer should be met constrains
        model.addConstrs((f_b.sum('*', '*', name) == 1 for name in customer_2b), name='2b_demand')
        model.addConstrs((f_c.sum('*', '*', name) == 1 for name in customer), name='demand')
        # the All CDC must supply items to RDC
        # model.addConstrs((f_c.sum(cdc_name, '*', '*') == 1 for cdc_name in cdc_cand), name='constrs')
        # model.addConstrs((f_b.sum(cdc_name, '*', '*') == 1 for cdc_name in cdc_cand), name='constrs')

        for cdc_name in cdc_cand:
            model.addConstrs((f_c.sum(cdc_name, '*', name) <= cdc_open[cdc_name] for name in customer), name='f_c_cons_cdc')
            model.addConstrs((f_b.sum(cdc_name, '*', name) <= cdc_open[cdc_name] for name in customer_2b), name='f_b_cons_cdc')
        for rdc_name in rdc_cand:
            model.addConstrs((f_c.sum('*', rdc_name, name) <= rdc_open[rdc_name] for name in customer), name='f_c_cons_rdc')
            model.addConstrs((f_b.sum('*', rdc_name, name) <= rdc_open[rdc_name] for name in customer_2b), name='f_b_cons_rdc')
        # the capacity constrains
        # if self.config.capacity_constrs_open:
        #     for rdc_name in rdc_cand:
        #         model.addConstr(sum((f_c.sum('*', rdc_name, name) * demand[name]['inventory_quantity'] for name in
        #                              customer)) +
        #                         sum((f_c.sum('*', rdc_name, name) * demand[name]['quantity'] for name in
        #                              customer)) * rr * self.config.return_turnover_days
        #                         <= rdc_open[rdc_name] * rdc_capacity[rdc_name], name='capacity_constr')
        #         model.addConstr(sum((f_c[rdc_name, name] * demand[name]['inventory_quantity'] for name in
        #                              customer)) +
        #                         sum((f_c[rdc_name, name] * demand[name]['quantity'] for name in
        #                              customer)) * rr * self.config.return_turnover_days
        #                         >= rdc_capacity[rdc_name], name='capacity_constr_1')

        # the toC sla constrains
        if self.config.sla_2c_constrs_open:
            SLA_NUM = 100
            for name in customer:
                model.addConstr(sum(f_c.sum('*', rdc_name, name) * distribution_price[rdc_name, name]['time_median_toC']
                                    for rdc_name in rdc_cand)
                    - sum(f_c.sum('*', rdc_name, name) * distribution_price[rdc_name, name]['sla_toC']
                                    for rdc_name in rdc_cand) <= SLA_NUM * (1 - sla[name]), name='sla_constr')
                model.addConstr(sum(f_c.sum('*', rdc_name, name) * distribution_price[rdc_name, name]['time_median_toC']
                                    for rdc_name in rdc_cand)
                     - sum(f_c.sum('*', rdc_name, name) * distribution_price[rdc_name, name]['sla_toC']
                                    for rdc_name in rdc_cand) >= -SLA_NUM * sla[name], name='sla_constr_1')
            model.addConstr(sum(sla[name] * demand[name]['demand_sum'] for name in customer) >=
                        self.config.P_c * sum(demand[name]['demand_sum'] for name in customer),
                        name='sla_constr_2')
        # toB sla  constrains
        if self.config.sla_2b_constrs_open:
            SLA_NUM = 100
            for name in customer_2b:
                model.addConstr(sum(f_b.sum('*', rdc_name, name) * distribution_price[rdc_name, name]['time_median_toB']
                                    for rdc_name in rdc_cand)
                    - sum(f_b.sum('*', rdc_name, name) * distribution_price[rdc_name, name]['sla_toB']
                                    for rdc_name in rdc_cand) <= SLA_NUM * (1 - sla_b[name]), name='sla_constr_2b')
                model.addConstr(sum(f_b.sum('*', rdc_name, name) * distribution_price[rdc_name, name]['time_median_toB']
                                    for rdc_name in rdc_cand)
                    - - sum(f_b.sum('*', rdc_name, name) * distribution_price[rdc_name, name]['sla_toB']
                                    for rdc_name in rdc_cand) >= -SLA_NUM * sla_b[name], name='sla_constr_2b_1')
            model.addConstr(sum(sla_b[name] * demand_2b[name]['demand_sum'] for name in customer_2b) >=
                        self.config.P_b * sum(demand_2b[name]['demand_sum'] for name in customer_2b),
                        name='sla_constr_2b_2')


        """######## define the objective function ######### """
        # define the shipping cost of cdc
        # intermediate variables
        q_cdc, w_cdc, q_cdc_rdc, w_cdc_rdc_B = self.cdc_b_temp_calc(f_b)
        q_cdc, w_cdc, q_cdc_rdc, w_cdc_rdc_C = self.cdc_c_temp_calc(f_c)
        cdc_shipping_cost, cdc_shipping_cost_r, cdc_shipping_cost_d, cdc_shipping_cost_r_d = \
            self.cdc_shipping_cost(w_cdc_rdc_C, w_cdc_rdc_B)

        i_rdc_C, q_rdc_C, w_rdc_C, i_rdc_c, q_rdc_c, w_rdc_c = self.rdc_c_temp_calc(f_c)
        i_rdc_B, q_rdc_B, w_rdc_B, i_rdc_b, q_rdc_b, w_rdc_b = self.rdc_b_temp_calc(f_b)
        rdc_shipping_cost, rdc_shipping_cost_d = self.rdc_shipping_cost(q_rdc_c, w_rdc_c, w_rdc_b)
        # rdc_storage_cost, rdc_storage_cost_d = self.rdc_storage_cost(i_rdc_c)
        # rdc_inbound, rdc_outbound, rdc_r_outbound, rdc_inbound_cost, rdc_outbound_cost, rdc_r_outbound_cost =\
        #     self.rdc_handling_cost(q_rdc_c)
        # rdc_capital_cost, rdc_capital_cost_d = self.capital_cost(i_rdc_c)
        # cost = cdc_shipping_cost + cdc_shipping_cost_r + rdc_shipping_cost + rdc_storage_cost\
        #        + rdc_inbound + rdc_outbound + rdc_r_outbound + rdc_capital_cost
        cost = rdc_shipping_cost + cdc_shipping_cost
        # set the Objectice function
        model.setObjective(cost, GRB.MINIMIZE)

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
            self.f_c = model.getAttr('x', f_c)
            self.f_b = model.getAttr('x', f_b)
            self.sla_b = model.getAttr('x', sla_b)
            self.sla = model.getAttr('x', sla)
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

    def cdc_shipping_cost(self, w_cdc_rdc_C, w_cdc_rdc_B):
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

        shipping_cost = 0
        shipping_cost_r = 0
        shipping_cost_d = {}
        shipping_cost_r_d = {}
        for cdc_name in cdc_rand:
            ship_c = 0
            for rdc_name in rdc_cand:
                # 小票零担
                tl_w_cdc_rdc = (w_cdc_rdc_C[cdc_name, rdc_name]+w_cdc_rdc_B[cdc_name, rdc_name]) * self.config.ltr / rdc_cost_loc[rdc_name]['replenishment']
                ship_tl_tmp = ((tl_w_cdc_rdc-trunk_price[cdc_name, rdc_name]['base_weight_qty_tl']) \
                                 * trunk_price[cdc_name, rdc_name]['weight_price_qty_tl'] \
                                  + trunk_price[cdc_name, rdc_name]['base_price_tl'])


                # 重货专运
                ftl_w_cdc_rdc = (w_cdc_rdc_C[cdc_name, rdc_name]+w_cdc_rdc_B[cdc_name, rdc_name]) * (1 - self.config.ltr) / rdc_cost_loc[rdc_name]['replenishment']

                ship_ftl_tmp = ((ftl_w_cdc_rdc - trunk_price[cdc_name, rdc_name]['base_weight_qty_ftl']) * \
                                   trunk_price[cdc_name, rdc_name]['weight_price_qty_ftl'] \
                                  + trunk_price[cdc_name, rdc_name]['base_price_ftl'])


                ship_c += (ship_tl_tmp + ship_ftl_tmp) * rdc_cost_loc[rdc_name]['replenishment']

            shipping_cost_d[cdc_name] = ship_c
            shipping_cost_r_d[cdc_name] = ship_c * self.config.rr_cdc
            shipping_cost += ship_c
        shipping_cost_r = shipping_cost * self.config.rr_cdc

        return shipping_cost, shipping_cost_r, shipping_cost_d, shipping_cost_r_d

    # TODO: shipping cost by order from rdc to customer
    def rdc_shipping_cost(self, d_rdc_c, w_rdc_C, w_rdc_B):
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
        distribution_price = self.data_class.distribution_price

        # declare variables
        shipping_cost_d = {}
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

            # toB customer
            ship_toB = 0
            for name in customer_2b:
                ship_toB += ((w_rdc_B[rdc_name, name] - distribution_price[rdc_name, name]['base_weight_qty_toB']) *
                              distribution_price[rdc_name, name]['weight_price_qty_toB']
                              + distribution_price[rdc_name, name]['base_price_toB'])

            shipping_cost_b += ship_toB
            # each RDC
            shipping_cost_d[rdc_name] = ship_toB + ship_less_toC + ship_more_toC
            # all RDC
        shipping_cost = shipping_cost_b + shipping_cost_c

        return shipping_cost, shipping_cost_d

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
        rdc_storage = 0
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
    def cdc_c_temp_calc(self, f_c):
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
        customer = self.data_class.customer
        demand = self.data_class.demand

        for cdc_name in cdc_cand:
            q_cdc_tmp_sum = 0
            w_cdc_tmp_sum = 0
            for rdc_name in rdc_cand:
                q_cdc_less_tmp = 0
                q_cdc_more_tmp = 0
                w_cdc_less_tmp = 0
                w_cdc_more_tmp = 0
                for name in customer:
                    q_cdc_less_tmp += f_c[cdc_name, rdc_name, name] * demand[name]['demand_L']
                    q_cdc_more_tmp += f_c[cdc_name, rdc_name, name] * demand[name]['demand_M']

                    w_cdc_less_tmp += f_c[cdc_name, rdc_name, name] * demand[name]['demand_L_weight']
                    w_cdc_more_tmp += f_c[cdc_name, rdc_name, name] * demand[name]['demand_M_weight']

                q_cdc_tmp_sum += q_cdc_less_tmp + q_cdc_more_tmp
                w_cdc_tmp_sum += w_cdc_less_tmp + w_cdc_more_tmp

                q_cdc_rdc[cdc_name, rdc_name] = q_cdc_less_tmp + q_cdc_more_tmp
                w_cdc_rdc[cdc_name, rdc_name] = w_cdc_less_tmp + w_cdc_more_tmp

            q_cdc[cdc_name] = q_cdc_tmp_sum
            w_cdc[cdc_name] = w_cdc_tmp_sum

        return q_cdc, w_cdc, q_cdc_rdc, w_cdc_rdc

    def cdc_b_temp_calc(self, f_b):
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
        customer = self.data_class.customer_2b
        demand = self.data_class.demand_2b

        for cdc_name in cdc_cand:
            q_cdc_tmp_sum = 0
            w_cdc_tmp_sum = 0
            for rdc_name in rdc_cand:
                q_cdc_less_tmp = 0
                w_cdc_less_tmp = 0
                for name in customer:
                    q_cdc_less_tmp += f_b[cdc_name, rdc_name, name] * demand[name]['demand_sum']
                    w_cdc_less_tmp += f_b[cdc_name, rdc_name, name] * demand[name]['demand_weight_sum']

                q_cdc_tmp_sum += q_cdc_less_tmp
                w_cdc_tmp_sum += w_cdc_less_tmp

                q_cdc_rdc[cdc_name, rdc_name] = q_cdc_less_tmp
                w_cdc_rdc[cdc_name, rdc_name] = w_cdc_less_tmp

            q_cdc[cdc_name] = q_cdc_tmp_sum
            w_cdc[cdc_name] = w_cdc_tmp_sum

        return q_cdc, w_cdc, q_cdc_rdc, w_cdc_rdc

    def rdc_c_temp_calc(self, f_c):
        """
        calculate the intermedidate variables from rdc to c
        :return:
        """
        # parameters
        rdc_cand = self.data_class.rdc_cand
        cdc_cand = self.data_class.cdc_cand
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
            i_rdc_sum = 0
            q_rdc_less_sum = 0
            q_rdc_more_sum = 0
            w_rdc_less_sum = 0
            w_rdc_more_sum = 0
            # toC customer
            for name in customer:
                q_rdc_less = 0
                q_rdc_more = 0
                w_rdc_less = 0
                w_rdc_more = 0
                w_rdc_avg = 0
                i_rdc_tmp = 0
                for cdc_name in cdc_cand:
                    q_rdc_less += f_c[cdc_name, rdc_name, name] * demand[name]['demand_L']
                    q_rdc_more += f_c[cdc_name, rdc_name, name] * demand[name]['demand_M']

                    w_rdc_less += f_c[cdc_name,rdc_name, name] * demand[name]['demand_L_weight']
                    w_rdc_more += f_c[cdc_name, rdc_name, name] * demand[name]['demand_M_weight']

                    i_rdc_tmp += (f_c[cdc_name, rdc_name, name] * demand[name]['demand_sum']) / total_days * turnover_days_c
                    w_rdc_avg += f_c[cdc_name, rdc_name, name] * demand[name]['demand_M_avg_weight']
                q_rdc_c[rdc_name, name] = {'less': q_rdc_less,
                                           'more': q_rdc_more
                                          }
                # TODO: comfirm
                w_rdc_c[rdc_name, name] = w_rdc_avg
                q_rdc_less_sum += q_rdc_less
                q_rdc_more_sum += q_rdc_more
                w_rdc_less_sum += w_rdc_less
                w_rdc_more_sum += w_rdc_more
                i_rdc_sum += i_rdc_tmp
                i_rdc_c[rdc_name, name] = i_rdc_tmp

            i_rdc[rdc_name] = i_rdc_sum
            q_rdc[rdc_name] = {'less': q_rdc_less_sum,
                               'more': q_rdc_more_sum,
                               }
            w_rdc[rdc_name] = w_rdc_less_sum + w_rdc_more_sum

        return i_rdc, q_rdc, w_rdc, i_rdc_c, q_rdc_c, w_rdc_c

        # post-process of cdc

    def rdc_b_temp_calc(self, f_b):
        """
        calculate the intermedidate variables from rdc to B

        :return:
        """
        # parameters
        total_days = 365
        cdc_cand = self.data_class.cdc_cand
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
            q_rdc_tmp_sum = 0
            w_rdc_tmp_sum = 0
            i_rdc_tmp_sum = 0
            for name in customer:
                q_rdc_tmp = 0
                w_rdc_tmp = 0
                i_rdc_tmp = 0
                for cdc_name in cdc_cand:
                    q_rdc_tmp += f_b[cdc_name, rdc_name, name] * demand[name]['demand_sum']
                    w_rdc_tmp += f_b[cdc_name, rdc_name, name] * demand[name]['demand_weight_sum']
                    i_rdc_tmp += (f_b[cdc_name, rdc_name, name] * demand[name]['demand_sum']) / total_days * turnover_days_b
                q_rdc_tmp_sum += q_rdc_tmp
                w_rdc_tmp_sum += w_rdc_tmp
                i_rdc_tmp_sum += i_rdc_tmp

                w_rdc_b[rdc_name, name] = w_rdc_tmp
                i_rdc_b[rdc_name, name] = i_rdc_tmp
                q_rdc_b[rdc_name, name] = q_rdc_tmp

            q_rdc[rdc_name] = q_rdc_tmp_sum
            w_rdc[rdc_name] = w_rdc_tmp_sum
            i_rdc[rdc_name] = i_rdc_tmp_sum

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
        f_c = self.f_c
        f_b = self.f_b
        q_cdc_B, w_cdc_B, q_cdc_rdc, w_cdc_rdc_B = self.cdc_b_temp_calc(f_b)
        q_cdc_C, w_cdc_C, q_cdc_rdc, w_cdc_rdc_C = self.cdc_c_temp_calc(f_c)
        cdc_shipping_cost, cdc_shipping_cost_r, cdc_shipping_cost_d, cdc_shipping_cost_r_d = \
            self.cdc_shipping_cost(w_cdc_rdc_C, w_cdc_rdc_B)
        m = 0
        for cdc_name in cdc_cand:
            cdc_output['CDC'] = cdc_name
            cdc_output['shipping_cost'] = cdc_shipping_cost_d[cdc_name] + cdc_shipping_cost_r_d[cdc_name]
            cdc_output['quantity'] = q_cdc_B[cdc_name] + q_cdc_C[cdc_name]
            cdc_output['weight'] = w_cdc_B[cdc_name] + w_cdc_C[cdc_name]
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

        rdc_open = self.rdc_open
        # # filter all items that cdc_open = 1, get the valid
        rdc_open_valid = {k:v for k,v in rdc_open.items() if v==1}

        # v_cdc_rdc, v_cdc = self.cdc_rdc_temp_calc(f_c)
        i_rdc_C, q_rdc_C, w_rdc_C, i_rdc_c, q_rdc_c, w_rdc_c = self.rdc_c_temp_calc(f_c)
        i_rdc_B, q_rdc_B, w_rdc_B, i_rdc_b, q_rdc_b, w_rdc_b = self.rdc_b_temp_calc(f_b)
        rdc_shipping_cost, rdc_shipping_cost_d = self.rdc_shipping_cost(q_rdc_c, w_rdc_c, w_rdc_b)
        rdc_storage_cost, rdc_storage_cost_d = self.rdc_storage_cost(i_rdc_c, i_rdc_b)
        # rdc_inbound, rdc_outbound, rdc_r_outbound, rdc_inbound_cost, rdc_outbound_cost, rdc_r_outbound_cost =\
        #     self.rdc_handling_cost(q_rdc_c)
        # rdc_capital_cost, rdc_capital_cost_d = self.capital_cost(i_rdc_c)

        m = 0
        for rdc_name in rdc_open_valid:
            rdc_output['RDC'] = rdc_name
            rdc_output['City_name'] = city_add[rdc_name]['city_name']
            rdc_output['shipping_cost'] = rdc_shipping_cost_d[rdc_name]
            rdc_output['storage_cost'] = rdc_storage_cost_d[rdc_name]
            # rdc_output['Inbound_Handling_Cost'] = rdc_inbound_cost[rdc_name]
            # rdc_output['Outbound_Handling_Cost'] = rdc_outbound_cost[rdc_name]
            # rdc_output['Reverse_Handling_Cost'] = rdc_r_outbound_cost[rdc_name]
            rdc_output['Inventory'] = i_rdc_C[rdc_name] + i_rdc_B[rdc_name]
            rdc_output['Area'] = rdc_output['Inventory'] / rdc_cost_loc[rdc_name]['unit_area']
            # rdc_output['Capital_Cost'] = rdc_capital_cost_d[rdc_name]
            # rdc_output['Volume'] = v_rdc[rdc_name]

            df = df.append(pd.DataFrame(rdc_output, index=[m]))
            m = m+1

        # TODO:
        rdc_inbound = 0
        rdc_outbound = 0
        rdc_r_outbound = 0
        rdc_capital_cost = 0
        return df, rdc_shipping_cost, rdc_storage_cost, rdc_inbound, rdc_outbound, rdc_r_outbound, rdc_capital_cost

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
        cdc_open = self.cdc_open
        rdc_open = self.rdc_open
        # filter all items that cdc_open = 1, get the valid
        cdc_open_valid = {k: v for k, v in cdc_open.items() if v == 1}
        rdc_open_valid = {k: v for k, v in rdc_open.items() if v == 1}
        f_c = self.f_c
        # loop
        k = 0
        df_c = pd.DataFrame()
        for cdc_name in cdc_open_valid.keys():
            c_network['CDC'] = cdc_name
            c_network['CDC_NAME'] = city_add[cdc_name]['city_name']
            c_network['CDC_LAT'] = city_add[cdc_name]['lat']
            c_network['CDC_LGT'] = city_add[cdc_name]['lgt']
            for rdc_name in rdc_open_valid.keys():
                c_network['RDC'] = rdc_name
                c_network['RDC_NAME'] = city_add[rdc_name]['city_name']
                c_network['RDC_LAT'] = city_add[rdc_name]['lat']
                c_network['RDC_LGT'] = city_add[rdc_name]['lgt']
                for c_name in customer:
                    if f_c[cdc_name, rdc_name, c_name] > 0.0:
                        c_network['CUSTOMER'] = c_name
                        c_network['QUANTITY'] = f_c[cdc_name, rdc_name, c_name] * demand[c_name]['demand_sum']
                        c_network['WEIGHT'] = f_c[cdc_name, rdc_name, c_name] * demand[c_name]['demand_weight_sum']
                        c_network['SLA'] = f_c[cdc_name, rdc_name, c_name] * distribution_price[rdc_name, c_name]['sla_toC']
                        c_network['CUSTOMER_LGT'] = city_add[c_name]['lgt']
                        c_network['CUSTOMER_LAT'] = city_add[c_name]['lat']
                        c_network['CUSTOMER_NAME'] = city_add[c_name]['city_name']
                        c_network['TIME'] = distribution_price[rdc_name, c_name]['time_median_toC']
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
        cdc_open = self.cdc_open
        # log.info('the cdc open is {}'.format(cdc_open.keys()))
        rdc_open = self.rdc_open
        # filter all items that cdc_open = 1, get the valid
        cdc_open_valid = {k: v for k, v in cdc_open.items() if v == 1}
        # log.info('the cdc open is {}'.format(cdc_open_valid.keys()))
        rdc_open_valid = {k: v for k, v in rdc_open.items() if v == 1}
        f_b = self.f_b
        # loop
        k = 0
        df_b = pd.DataFrame()
        for cdc_name in cdc_open_valid.keys():
            c_network['CDC'] = cdc_name
            c_network['CDC_NAME'] = city_add[cdc_name]['city_name']
            c_network['CDC_LAT'] = city_add[cdc_name]['lat']
            c_network['CDC_LGT'] = city_add[cdc_name]['lgt']
            for rdc_name in rdc_open_valid.keys():
                c_network['RDC'] = rdc_name
                c_network['RDC_NAME'] = city_add[rdc_name]['city_name']
                c_network['RDC_LAT'] = city_add[rdc_name]['lat']
                c_network['RDC_LGT'] = city_add[rdc_name]['lgt']
                for c_name in customer:
                    # log.info('cdc_name before {}'.format(cdc_name))
                    if f_b[cdc_name, rdc_name, c_name] > 0.0:
                        # log.info('cdc_name after {}'.format(cdc_name))
                        c_network['CUSTOMER'] = c_name
                        c_network['QUANTITY'] = f_b[cdc_name, rdc_name, c_name] * demand[c_name]['demand_sum']
                        c_network['WEIGHT'] = f_b[cdc_name, rdc_name, c_name] * demand[c_name]['demand_weight_sum']
                        c_network['SLA'] = f_b[cdc_name, rdc_name, c_name] * distribution_price[rdc_name, c_name][
                            'sla_toC']
                        c_network['CUSTOMER_LGT'] = city_add[c_name]['lgt']
                        c_network['CUSTOMER_LAT'] = city_add[c_name]['lat']
                        c_network['CUSTOMER_NAME'] = city_add[c_name]['city_name']
                        c_network['TIME'] = distribution_price[rdc_name, c_name]['time_median_toC']
                        df_b = df_b.append(pd.DataFrame(c_network, index=[k]))
                        k = k + 1
                else:
                    continue
        return df_b





