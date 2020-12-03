#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
# @Time    : 2020/11/17 10:48
# @Author  : peng.wang
# @Email   : WangPeng4@sfmail.sf-express.com
# @FileName: model.py
# @ProjectName :Prediction_Optimization
"""

import pandas as pd
from gurobipy import *
from utils.misc import Logger
from collections import defaultdict
# define the log file
log = Logger(log_path='../log').logger

REC_COST = 1
SEND_COST = 1
REC_DISCOUNT_COST = 18
SEND_DISCOUNT_COST = 12


class Scheduler(object):
    """
    根据各个区域的收派件量进行小哥分配优化，使得整个成本最小
    """
    def __init__(self, config, data):
        self.config = config
        self.data = data
        self.allocation = None
        self.send_discount = None
        self.rec_discount = None

    def resource_scheduler_deterministic(self):
        """

        :return:
        """
        # Params
        basic_cost = self.data.basic_cost
        capacity = self.data.capacity
        # capacity_cost = self.data.capacity_cost

        # Data
        zones = self.data.zones
        employees = self.data.employees
        demands = self.data.demands_daily

        # Model
        model = Model('scheduler optimization model')

        # Variables
        allocation = model.addVars(employees, zones, vtype=GRB.BINARY, name='allocation')
        rec_quantity = model.addVars(zones, vtype=GRB.INTEGER, name="rec_quantity")
        send_quantity = model.addVars(zones, vtype=GRB.INTEGER, name="send_quantity")
        rec_discount = model.addVars(zones, vtype=GRB.INTEGER, name="receive_discount")
        send_discount = model.addVars(zones, vtype=GRB.INTEGER, name="send_discount")

        # Constraints
        model.addConstrs((allocation.sum(name, '*') <= 1 for name in employees), name='zone constraints')
        for zone in zones:
            model.addConstr(quicksum(allocation[em, zone]*capacity[em]['receive'] for em in employees) >= rec_quantity[zone]
                            , name='receive_quantity')
            model.addConstr(quicksum(allocation[em, zone]*capacity[em]['send'] for em in employees) >= send_quantity[zone]
                            , name='send_quantity')
            model.addConstr(rec_quantity[zone] + rec_discount[zone] == demands[zone]['receive'],
                            name='receive capacity constratins')
            model.addConstr(send_quantity[zone] + send_discount[zone] == demands[zone]['send'],
                            name='send capacity constratins')

        model.update()
        # objectives
        objectives = quicksum(quicksum(allocation[em, zone]*basic_cost[em] for em in employees)
                              + rec_quantity[zone] * REC_COST
                              + send_quantity[zone] * SEND_COST
                              + rec_discount[zone]*REC_DISCOUNT_COST
                              + send_discount[zone]*SEND_DISCOUNT_COST
                              for zone in zones)
        # set the params
        model.setParam('MIPGap', 0.005)
        model.setParam('TimeLimit', 100)
        model.update()
        # set the target
        model.setObjective(objectives, GRB.MINIMIZE)
        # optimize the model
        model.optimize()


        if model.Status == GRB.INFEASIBLE:

            try:
                model.computeIIS()
                model.write("model1.ilp")
            except GurobiError:
                print('feasible model')

        elif model.Status == GRB.OPTIMAL:
            log.info('the facility location optimized sucessfully !')
            log.info('the objective of the model is {}'.format(model.objVal))
            model.write("model1_correct.lp")
            self.allocation = model.getAttr('x', allocation)
            self.rec_discount = model.getAttr('x', rec_discount)
            self.send_discount = model.getAttr('x', send_discount)
            self.rec_quantity = model.getAttr('x', rec_quantity)
            self.send_quantity = model.getAttr('x', send_quantity)
        return model

    def scheduler_result(self):
        """

        :return:
        """
        zones = self.data.zones
        employees = self.data.employees
        demands = self.data.demands_daily
        capacity = self.data.capacity
        result_df = pd.DataFrame()
        result_dict = {}
        k=0
        for zone in zones:
            result_dict['区域id'] = zone
            result_dict['取件量'] = self.rec_quantity[zone]
            result_dict['派件量'] = self.send_quantity[zone]
            result_dict['剩余取件量'] = self.rec_discount[zone]
            result_dict['剩余派件量'] = self.send_discount[zone]
            result_dict['取件需求量'] = demands[zone]['receive']
            result_dict['派件需求量'] = demands[zone]['send']

            for em in employees:
                if self.allocation[em, zone] > 0.5:
                    result_dict['小哥id'] = em
                    result_dict['小哥收件能力'] = capacity[em]['receive']
                    result_dict['小哥派件能力'] = capacity[em]['send']
                    result_df = result_df.append(pd.DataFrame(result_dict, index=[k]))
            k = k+1

        return result_df




    def scheduler_solution(self):
        """

        :return:
        """
        zones = self.data.zones
        employees = self.data.employees

        employee_num = 0
        solution = defaultdict(list)
        for zone in zones:
            for em in employees:
                if self.allocation[em, zone] > 0.6:
                    solution[zone].append(em)
                    employee_num += 1
        return solution, employee_num

    def total_cost(self):
        """
        计算总成本以及各项分成本
        :return:
        """
        zones = self.data.zones
        employees = self.data.employees
        basic_cost = self.data.basic_cost
        # capacity = self.data.capacity
        total_basic_cost = 0
        total_payment = 0
        total_discount_payment = 0

        for zone in zones:
            total_payment += self.rec_quantity[zone] + self.send_quantity[zone]
            total_discount_payment += self.rec_discount[zone] * REC_DISCOUNT_COST \
                                      + self.send_discount[zone] * SEND_DISCOUNT_COST
            for em in employees:
                if self.allocation[em, zone] > 0.5:
                    total_basic_cost += self.allocation[em, zone] * basic_cost[em]

        total_cost = total_discount_payment + total_basic_cost + total_payment
        return total_cost, total_basic_cost, total_payment, total_discount_payment

    def resource_scheduler_expected(self):
        """

        :return:
        """
        # Params
        basic_cost = self.data.basic_cost
        capacity = self.data.capacity
        capacity_cost = self.data.capacity_cost

        # Data
        zones = self.data.zones
        employees = self.data.employees
        demands = self.data.demands
        samples = 10000
        # Model
        model = Model('scheduler optimization model')

        # Variables
        allocation = model.addVars(employees, zones, vtype=GRB.BINARY, name='allocation')
        rec_discount = model.addVars(zones, samples, vtype=GRB.CONTINUOUS, name="receive_discount")
        send_discount = model.addVars(zones, samples, vtype=GRB.CONTINUOUS, name="send_discount")

        # Constraints
        model.addConstrs((allocation.sum(name, '*') <= 1 for name in employees), name='zone constraints')
        for zone in zones:
            for i in range(samples):
                model.addConstr(quicksum(allocation[em, zone]*capacity['receive'][em] for em in employees)
                                + rec_discount[zone, i] >= demands['receive'][zone][i], name='receive capacity constratins')
                model.addConstr(quicksum(allocation[em, zone]*capacity['send'][em] for em in employees)
                                + send_discount[zone, i] >= demands['send'][zone][i], name='send capacity constratins')

        model.update()
        # objectives
        objectives = quicksum(quicksum(allocation[em, zone]*basic_cost[em]
                                       + allocation[em, zone]*capacity['receive'][em]*capacity_cost['receive'][em]
                                       + allocation[em, zone]*capacity['send'][em]*capacity_cost['send'][em]
                                       + rec_discount[zone]*REC_DISCOUNT_COST
                                       + send_discount[zone]*SEND_DISCOUNT_COST
                                       for em in employees) for zone in zones)

        # set the target
        model.setObjective(objectives, GRB.MINIMIZE)
        # optimize the model
        model.optimize()

        if model.Status == GRB.INFEASIBLE:

            try:
                model.computeIIS()
                model.write("model1.ilp")
            except GurobiError:
                print('feasible model')

        else:
            model.write("model1_correct.lp")
            self.allocation = model.getAttr('x', allocation)
            self.rec_discount = model.getAttr('x', rec_discount)
            self.send_discount = model.getAttr('x', send_discount)

        return model

    def resource_scheduler_expected_CVaR(self):
        """

        :return:
        """
        # Params
        basic_cost = self.data.basic_cost
        capacity = self.data.capacity
        capacity_cost = self.data.capacity_cost

        # Data
        zones = self.data.zones
        employees = self.data.employees
        demands = self.data.demands

        # Model
        model = Model('scheduler optimization model')

        # Variables
        allocation = model.addVars(employees, zones, vtype=GRB.BINARY, name='allocation')
        rec_discount = model.addVars(zones, vtype=GRB.CONTINUOUS, name="receive_discount")
        send_discount = model.addVars(zones, vtype=GRB.CONTINUOUS, name="send_discount")

        # Constraints
        model.addConstrs((allocation.sum(name, '*') <= 1 for name in employees), name='zone constraints')
        for zone in zones:
            model.addConstr(quicksum(allocation[em, zone]*capacity['receive'][em] for em in employees)
                            + rec_discount[zone] >= demands['receive'][zone], name='receive capacity constratins')
            model.addConstr(quicksum(allocation[em, zone]*capacity['send'][em] for em in employees)
                            + send_discount[zone] >= demands['send'][zone], name='send capacity constratins')

        model.update()
        # objectives
        objectives = quicksum(quicksum(allocation[em, zone]*basic_cost[em]
                                       + allocation[em, zone]*capacity['receive'][em]*capacity_cost['receive'][em]
                                       + allocation[em, zone]*capacity['send'][em]*capacity_cost['send'][em]
                                       + rec_discount[zone]*REC_DISCOUNT_COST
                                       + send_discount[zone]*SEND_DISCOUNT_COST
                                       for em in employees) for zone in zones)

        # set the target
        model.setObjective(objectives, GRB.MINIMIZE)
        # optimize the model
        model.optimize()

        if model.Status == GRB.INFEASIBLE:

            try:
                model.computeIIS()
                model.write("model1.ilp")
            except GurobiError:
                print('feasible model')

        else:
            model.write("model1_correct.lp")
            self.allocation = model.getAttr('x', allocation)
            self.rec_discount = model.getAttr('x', rec_discount)
            self.send_discount = model.getAttr('x', send_discount)

        return model
