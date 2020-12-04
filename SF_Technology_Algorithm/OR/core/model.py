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
import random
random.seed(a=100)    #设置随机生成器的种子，保证可重复性
# define the log file
log = Logger(log_path='../log').logger

REC_COST = 1
SEND_COST = 1
REC_DISCOUNT_COST = 18
SEND_DISCOUNT_COST = 12
SAMPLES = 4
SIGMA_REC = 12
SIGMA_SEND = 12
MU = 0


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
        self.rec_quantity = None
        self.send_quantity = None

    def resource_scheduler_initial(self):
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
        rec_quantity = model.addVar(lb=0, vtype=GRB.INTEGER, name="rec_quantity")
        send_quantity = model.addVar(lb=0, vtype=GRB.INTEGER, name="send_quantity")
        rec_discount = model.addVar(lb=0, vtype=GRB.INTEGER, name="receive_discount")
        send_discount = model.addVar(lb=0, vtype=GRB.INTEGER, name="send_discount")

        # Constraints
        model.addConstrs((allocation.sum(name, '*') <= 1 for name in employees), name='zone constraints')

        model.addConstr(quicksum(quicksum(allocation[em, zone]*capacity[em]['receive'] for em in employees) for zone in zones) >= rec_quantity
                        , name='receive_quantity')
        model.addConstr(quicksum(quicksum(allocation[em, zone]*capacity[em]['send'] for em in employees) for zone in zones) >= send_quantity
                            , name='send_quantity')
        if self.config.target == 'predict':
            log.info('using the predicted value as optimized target')
            model.addConstr(rec_quantity + rec_discount >= sum(demands[zone]['receive_predict'] for zone in zones),
                                name='receive capacity constratins')
            model.addConstr(send_quantity + send_discount >= sum(demands[zone]['send_predict'] for zone in zones),
                                name='send capacity constratins')
        else:
            log.info('using the reality value as optimized target')
            model.addConstr(rec_quantity + rec_discount >= sum(demands[zone]['receive'] for zone in zones),
                                name='receive capacity constratins')
            model.addConstr(send_quantity + send_discount >= sum(demands[zone]['send'] for zone in zones),
                                name='send capacity constratins')

        model.update()
        # objectives
        objectives = quicksum(quicksum(allocation[em, zone]*basic_cost[em] for em in employees) for zone in zones) \
                     + rec_quantity * REC_COST \
                     + send_quantity * SEND_COST \
                     + rec_discount * REC_DISCOUNT_COST \
                     + send_discount * SEND_DISCOUNT_COST

        # set the params
        model.setParam('MIPGap', 0.005)
        model.setParam('TimeLimit', 200)
        # set the target
        model.setObjective(objectives, GRB.MINIMIZE)
        # optimize the model
        model.optimize()

        if model.Status == GRB.INFEASIBLE:

            try:
                model.computeIIS()
                model.write("model1.ilp")
            except GurobiError:
                log.info('infeasible model')

        else:
            log.info('the facility location optimized sucessfully !')
            log.info('the objective of the model is {}'.format(model.objVal))
            model.write("model1_correct.lp")
            self.allocation = model.getAttr('x', allocation)
            self.rec_discount = model.getAttr('x', rec_discount)
            self.send_discount = model.getAttr('x', send_discount)
            self.rec_quantity = model.getAttr('x', rec_quantity)
            self.send_quantity = model.getAttr('x', send_quantity)
        return model

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
        log.info('using the {} value as optimized target'.format(self.config.target))
        for zone in zones:
            model.addConstr(quicksum(allocation[em, zone]*capacity[em]['receive'] for em in employees) >= rec_quantity[zone]
                            , name='receive_quantity')
            model.addConstr(quicksum(allocation[em, zone]*capacity[em]['send'] for em in employees) >= send_quantity[zone]
                            , name='send_quantity')
            if self.config.target == "predict":
                model.addConstr(rec_quantity[zone] + rec_discount[zone] == demands[zone]['receive_predict'],
                                name='receive capacity constratins')
                model.addConstr(send_quantity[zone] + send_discount[zone] == demands[zone]['send_predict'],
                                name='send capacity constratins')
            else:
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
        model.setParam('TimeLimit', 200)
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
                log.info('infeasible model')

        else:
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
        k = 0
        for zone in zones:
            result_dict['区域id'] = zone
            result_dict['取件量'] = self.rec_quantity[zone]
            result_dict['派件量'] = self.send_quantity[zone]
            result_dict['剩余取件量'] = self.rec_discount[zone]
            result_dict['剩余派件量'] = self.send_discount[zone]
            result_dict['取件需求量预测'] = demands[zone]['receive_predict']
            result_dict['派件需求量预测'] = demands[zone]['send_predict']
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

        # Data
        zones = self.data.zones
        employees = self.data.employees
        demands = self.data.demands_daily
        receive_samples = self.data.receive_samples_daily
        send_samples = self.data.send_samples_daily
        samples = SAMPLES
        # samples_prob = [0.1, 0.1,0.1,0.1,0.1,0.4,0.]
        # Generate the predicted error samples
        sigma_rec = SIGMA_REC
        sigma_send = SIGMA_SEND
        mu = MU  # 均值
        # predict_errors_rec = [max(int(random.normalvariate(mu, sigma_rec)), 0) for i in range(samples)]   # 取正值
        # predict_errors_send =[max(int(random.normalvariate(mu, sigma_send)), 0) for i in range(samples)]   # 取正值
        # predict_errors_rec = [int(random.normalvariate(mu, sigma_rec)) for i in range(samples)]   # 取正负值
        # predict_errors_send =[int(random.normalvariate(mu, sigma_send)) for i in range(samples)]   # 取正负值

        # Model
        model = Model('scheduler optimization model')
        log.info('running the stochastic programming with expected metric')
        # Variables
        allocation = model.addVars(employees, zones, vtype=GRB.BINARY, name='allocation')
        rec_quantity = model.addVars(zones, samples, vtype=GRB.INTEGER, name="rec_quantity")
        send_quantity = model.addVars(zones, samples, vtype=GRB.INTEGER, name="send_quantity")
        rec_discount = model.addVars(zones, samples, vtype=GRB.INTEGER, name="receive_discount")
        send_discount = model.addVars(zones, samples, vtype=GRB.INTEGER, name="send_discount")

        # Constraints
        model.addConstrs((allocation.sum(name, '*') <= 1 for name in employees), name='zone_constraints')
        for zone in zones:
            for sample in range(samples):
                model.addConstr(
                    quicksum(allocation[em, zone] * capacity[em]['receive'] for em in employees) >=
                    rec_quantity[zone, sample], name='receive_quantity')
                model.addConstr(
                    quicksum(allocation[em, zone] * capacity[em]['send'] for em in employees) >=
                    send_quantity[zone, sample], name='send_quantity')
                model.addConstr(rec_quantity[zone, sample] + rec_discount[zone, sample] ==
                                receive_samples[receive_samples['区域id']==zone].values.flatten()[sample+1],
                                name='receive capacity constratins')
                model.addConstr(send_quantity[zone, sample] + send_discount[zone, sample] ==
                                send_samples[send_samples['区域id']==zone].values.flatten()[sample+1],
                                name='send capacity constratins')

        model.update()
        # objectives
        objectives = quicksum(quicksum(allocation[em, zone]*basic_cost[em] for em in employees) for zone in zones) \
                     + quicksum(rec_quantity[zone, sample] * REC_COST + send_quantity[zone, sample] * SEND_COST
                                + rec_discount[zone, sample]*REC_DISCOUNT_COST
                                + send_discount[zone, sample]*SEND_DISCOUNT_COST
                                for sample in range(samples) for zone in zones)* 1/samples
        # set the params
        model.setParam('MIPGap', 0.005)
        model.setParam('TimeLimit', 600)
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
                log.info('infeasible model')

        else:
            model.write("model1_correct.lp")
            self.allocation = model.getAttr('x', allocation)
            self.rec_discount = model.getAttr('x', rec_discount)
            self.send_discount = model.getAttr('x', send_discount)
            self.rec_quantity = model.getAttr('x', rec_quantity)
            self.send_quantity = model.getAttr('x', send_quantity)

        return model

    def resource_scheduler_expected_cvar(self):
        """

        :return:
        """
        # Params
        alpha = 0.75
        basic_cost = self.data.basic_cost
        capacity = self.data.capacity

        # Data
        zones = self.data.zones
        employees = self.data.employees
        demands = self.data.demands_daily
        samples = SAMPLES

        # Generate the predicted error samples
        sigma_rec = SIGMA_REC
        sigma_send = SIGMA_SEND
        mu = MU  # 均值
        predict_errors_rec = [max(int(random.normalvariate(mu, sigma_rec)), 0) for i in range(samples)]   # 取正值
        predict_errors_send =[max(int(random.normalvariate(mu, sigma_send)), 0) for i in range(samples)]   # 取正值
        # Model
        model = Model('scheduler optimization model')
        log.info('running the stochastic programming with cvar metric')
        # Variables
        allocation = model.addVars(employees, zones, vtype=GRB.BINARY, name='allocation')
        rec_quantity = model.addVars(zones, samples, vtype=GRB.INTEGER, name="rec_quantity")
        send_quantity = model.addVars(zones, samples, vtype=GRB.INTEGER, name="send_quantity")
        rec_discount = model.addVars(zones, samples, vtype=GRB.INTEGER, name="receive_discount")
        send_discount = model.addVars(zones, samples, vtype=GRB.INTEGER, name="send_discount")
        # new variables for conditioned value at risk
        total_cost = model.addVar(vtype=GRB.CONTINUOUS, name='total_cost')
        excess = model.addVars(samples, vtype=GRB.CONTINUOUS, name='cost_excess')
        profit = model.addVars(samples, vtype=GRB.CONTINUOUS, name='profit')


        # Constraints
        model.addConstrs((allocation.sum(name, '*') <= 1 for name in employees), name='zone constraints')
        for zone in zones:
            for sample in range(samples):
                model.addConstr(
                    quicksum(allocation[em, zone] * capacity[em]['receive'] for em in employees) >=
                    rec_quantity[zone, sample], name='receive_quantity')
                model.addConstr(
                    quicksum(allocation[em, zone] * capacity[em]['send'] for em in employees) >=
                    send_quantity[zone, sample], name='send_quantity')
                model.addConstr(rec_quantity[zone, sample] + rec_discount[zone, sample] == demands[zone]['receive']
                                + predict_errors_rec[sample],
                                name='receive capacity constratins')
                model.addConstr(send_quantity[zone, sample] + send_discount[zone, sample] == demands[zone]['send']
                                + predict_errors_send[sample],
                                name='send capacity constratins')
        # constraints for cvar
        for sample in range(samples):
            model.addConstr(profit[sample] ==
                         quicksum(quicksum(allocation[em, zone]*basic_cost[em] for em in employees) for zone in zones)
                         + quicksum(rec_quantity[zone, sample] * REC_COST + send_quantity[zone, sample] * SEND_COST
                                + rec_discount[zone, sample]*REC_DISCOUNT_COST
                                + send_discount[zone, sample]*SEND_DISCOUNT_COST
                                for zone in zones))
            model.addConstr(profit[sample]-total_cost <= excess[sample])
        model.update()
        # objectives
        # objectives = quicksum(quicksum(allocation[em, zone]*basic_cost[em] for em in employees) for zone in zones) \
        #              + quicksum(rec_quantity[zone, sample] * REC_COST + send_quantity[zone, sample] * SEND_COST
        #                         + rec_discount[zone, sample]*REC_DISCOUNT_COST
        #                         + send_discount[zone, sample]*SEND_DISCOUNT_COST
        #                         for sample in range(samples) for zone in zones)* 1/samples
        objectives = total_cost + 1.0 / (1-alpha)*samples * quicksum(excess[sample] for sample in range(samples))
        # set the params
        model.setParam('MIPGap', 0.005)
        model.setParam('TimeLimit', 600)
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
                log.info('infeasible model')

        else:
            model.write("model1_correct.lp")
            self.allocation = model.getAttr('x', allocation)
            self.rec_discount = model.getAttr('x', rec_discount)
            self.send_discount = model.getAttr('x', send_discount)
            self.rec_quantity = model.getAttr('x', rec_quantity)
            self.send_quantity = model.getAttr('x', send_quantity)

        return model
