'''
# @Time        :2020/11/20 17:54
# @Author      :ChunRong.Chen
# @ FileName   :optimization.py  
'''
from collections import  defaultdict
from  datetime import  datetime ,timedelta, date
from utils.sensitivity_analysis import Sensitivity
from utils.util import DataHandler
import json
import  pandas as pd
import matplotlib.pyplot as plt
import  numpy as np
import  random
import copy
from utils.misc import Logger
log = Logger(log_path='../log').logger

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
REC_COST = 1
SEND_COST = 1
REC_DISCOUNT_COST = 18
SEND_DISCOUNT_COST = 12

class Optimization(object):
    def __init__(self, data_ins,solution_path):
        self.data = data_ins
        self.solution_path = solution_path


    def read_dataStru_json(self):
        with open(self.solution_path, 'r', encoding='utf-8') as load_f:
            strF = load_f.read()
            if len(strF) > 0:
                datas = json.loads(strF)
            else:
                datas = {}
        return datas


    def cost_json(self, day,strategy):

        cost_dict = defaultdict(int)
        daily_cost = 0

        for zone, employees in strategy.items():
            demand_r, demand_s, cap_r, cap_s = 0, 0, 0, 0
            cost_zone, basic_cost = 0, 0
            if isinstance(day,str):
                day = datetime.strptime(day,'%Y-%m-%d')
            if not type(day) is date:
                day  = datetime.date(day)
            # TODO: 修改为预测值
            demand_r = self.data.demands[day][zone]['receive_predict']
            demand_s = self.data.demands[day][zone]['send_predict']
            for employee in employees:
                cap_r += self.data.capacity[employee]['receive']
                cap_s += self.data.capacity[employee]['send']
                basic_cost += self.data.basic_cost[employee]
            real_r = min(cap_r, demand_r)
            real_s = min(cap_s, demand_s)
            dis_r = max(0, demand_r - cap_r)
            dis_s = max(0, demand_s - cap_s)
            cost_zone = (basic_cost  # 基础雇佣成本
                         + real_r * REC_COST  # 收件成本
                         + real_s * SEND_COST  # 派件成本
                         + dis_r * REC_DISCOUNT_COST  # 收件惩罚成本
                         + dis_s * SEND_DISCOUNT_COST)  # 派件惩罚成本
            daily_cost += cost_zone
            cost_dict[zone] = cost_zone



        return (daily_cost)

    def algorithm(self,day,strategy):
        zone = list(strategy.keys())
        em_ori = self.data.employees

        obj_add,obj_sub,obj_rep = float('inf'),float('inf'),float('inf')
        # 初始化

            # 禁忌表
        em_taboo_len = 5
        zone_taboo_len = 3
            # 迭代次数
        maxGen = 100000

        once_gen = 1
        gen = 0

        em_taboo = []
        zone_taboo = []
            # 未工作/工作小哥
        em_idle = []
        em_work = []
        for i in (list(strategy.values())):
            em_work.extend(i)
        em_idle = list(set(em_ori).difference(set(em_work)))
        obj = self.cost_json(day,strategy)
        print(day,obj)
        log.info('开始迭代过程')
        while gen<maxGen:
            if  not gen%100:
                print('generation {}'.format(gen))
            zone_selected = random.sample(set(zone).difference(set(zone_taboo)), 1)[0]
            em_selected_add = random.sample(set(em_idle).difference(set(em_taboo)), 1)[0]
            em_selected_sub = random.sample(set(strategy[zone_selected]).difference(set(em_taboo)), 1)[0]
            if em_selected_sub == em_selected_add:
                pass

            # 缓存变量
            tmp_add_em_work =  copy.deepcopy(em_work)
            tmp_add_em_idle = copy.deepcopy(em_idle)
            tmp_sub_em_work = copy.deepcopy(em_work)
            tmp_sub_em_idle = copy.deepcopy(em_idle)
            tmp_rep_em_work = copy.deepcopy(em_work)
            tmp_rep_em_idle = copy.deepcopy(em_idle)

            tmp_add_strategy = copy.deepcopy(strategy)
            tmp_add_em_taboo = copy.deepcopy(em_taboo)
            tmp_add_zone_taboo = copy.deepcopy(zone_taboo)
            tmp_sub_strategy = copy.deepcopy(strategy)
            tmp_sub_em_taboo = copy.deepcopy(em_taboo)
            tmp_sub_zone_taboo = copy.deepcopy(zone_taboo)
            tmp_rep_strategy = copy.deepcopy(strategy)
            tmp_rep_em_taboo = copy.deepcopy(em_taboo)
            tmp_rep_zone_taboo = copy.deepcopy(zone_taboo)


            # 增加
            tmp_add_strategy[zone_selected].append(em_selected_add)
            tmp_add_zone_taboo.append(zone_selected)
            tmp_add_zone_taboo = tmp_add_zone_taboo[0:zone_taboo_len]
            tmp_add_em_taboo.append(em_selected_add)
            tmp_add_em_taboo = tmp_add_em_taboo[0:em_taboo_len]
            obj_add = self.cost_json(day,tmp_add_strategy)
            tmp_add_em_work.append(em_selected_add)
            tmp_add_em_idle.remove(em_selected_add)

            # 替换


            if em_selected_sub in tmp_rep_strategy[zone_selected]:
                tmp_rep_strategy[zone_selected].remove(em_selected_sub)
                tmp_rep_strategy[zone_selected].append(em_selected_add)
                tmp_rep_zone_taboo.append(zone_selected)
                tmp_rep_zone_taboo = tmp_rep_zone_taboo[0:zone_taboo_len]
                tmp_rep_em_taboo.append(em_selected_sub)
                tmp_rep_em_taboo.append(em_selected_add)
                tmp_rep_em_taboo = tmp_rep_em_taboo[0:em_taboo_len]

                tmp_rep_em_work.append(em_selected_add)
                tmp_rep_em_idle.remove(em_selected_add)
                # if em_selected_sub not in tmp_rep_em_work:
                #     print(tmp_sub_em_work)
                #     print(em_selected_sub)
                # else:
                #     tmp_rep_em_work.remove(em_selected_sub)

                tmp_rep_em_work.remove(em_selected_sub)
                tmp_rep_em_idle.append(em_selected_sub)

                obj_rep = self.cost_json(day,tmp_rep_strategy)


            # 删除
            if em_selected_sub in tmp_sub_strategy[zone_selected]:
                tmp_sub_strategy[zone_selected].remove(em_selected_sub)
                tmp_sub_zone_taboo.append(zone_selected)
                tmp_sub_zone_taboo = tmp_sub_zone_taboo[0:zone_taboo_len]
                tmp_sub_em_taboo.append(em_selected_sub)
                tmp_sub_em_taboo = tmp_sub_em_taboo[0:em_taboo_len]
                #
                # if em_selected_sub not in tmp_sub_em_work:
                #     print(tmp_sub_em_work)
                #     print(em_selected_sub)
                # else:
                #     tmp_sub_em_work.remove(em_selected_sub)

                tmp_sub_em_work.remove(em_selected_sub)
                tmp_sub_em_idle.append(em_selected_sub)
                obj_sub = self.cost_json(day,tmp_sub_strategy)

            # 更新

            obj_list = [obj_add,obj_rep,obj_sub]
            tmp_best_index = obj_list.index(min(obj_list))
            tmp_best_obj = min(obj_list)
            if tmp_best_obj<=obj:
                if tmp_best_index ==0:
                    strategy = tmp_add_strategy
                    zone_taboo = tmp_add_zone_taboo
                    em_taboo =  tmp_add_em_taboo
                    obj = obj_add
                    em_work = tmp_add_em_work
                    em_idle =tmp_add_em_idle
                    log.info('{} is add into {}'.format(em_selected_add,zone_selected))

                elif tmp_best_index ==1:
                    strategy = tmp_rep_strategy
                    zone_taboo = tmp_rep_zone_taboo
                    em_taboo = tmp_rep_em_taboo
                    obj = obj_rep
                    em_work = tmp_rep_em_work
                    em_idle = tmp_rep_em_idle
                    log.info('{} is replaced by {} to {}'.format(em_selected_sub,em_selected_add, zone_selected))

                elif tmp_best_index == 2:
                    strategy = tmp_sub_strategy
                    zone_taboo = tmp_sub_zone_taboo
                    em_taboo = tmp_sub_em_taboo
                    obj = obj_sub
                    em_work = tmp_sub_em_work
                    em_idle = tmp_sub_em_idle
                    log.info('{} is deleted from {}'.format(em_selected_sub, zone_selected))

            gen+=once_gen

        return (strategy,obj)

    def algorithm1(self,day,strategy):
        zone = list(strategy.keys())
        em_ori = self.data.employees

        obj_tmp =float('inf')
        # 初始化
            # 随机因子
        random_index = [1/3,2/3,1]
            # 禁忌表
        em_taboo_len = 6
        zone_taboo_len = 3
            # 迭代次数
        maxGen = 100000

        once_gen = 30
        gen = 0

        em_taboo = []
        zone_taboo = []
            # 未工作/工作小哥
        em_idle = []
        em_work = []
        for i in (list(strategy.values())):
            em_work.extend(i)
        em_idle = list(set(em_ori).difference(set(em_work)))
        obj = self.cost_json(day,strategy)
        print(day,obj)
        log.info('开始迭代过程')
        while gen<maxGen:
            if  not gen%10000:
                print('generation {}'.format(gen))
            for j in range(random.randint(0,once_gen)):
                zone_selected = random.sample(set(zone).difference(set(zone_taboo)), 1)[0]
                em_selected_add = random.sample(set(em_idle).difference(set(em_taboo)), 1)[0]
                em_selected_sub = random.sample(set(strategy[zone_selected]).difference(set(em_taboo)), 1)[0]
                random_one = random.random()
                tmp_log = []

                # 缓存变量
                tmp_em_work =  copy.deepcopy(em_work)
                tmp_em_idle = copy.deepcopy(em_idle)

                tmp_strategy = copy.deepcopy(strategy)
                tmp_em_taboo = copy.deepcopy(em_taboo)
                tmp_zone_taboo = copy.deepcopy(zone_taboo)

                if random_one <=random_index[0]:
                    # 增加
                    tmp_strategy[zone_selected].append(em_selected_add)
                    tmp_zone_taboo.append(zone_selected)
                    tmp_zone_taboo = tmp_zone_taboo[0:zone_taboo_len]
                    tmp_em_taboo.append(em_selected_add)
                    tmp_em_taboo = tmp_em_taboo[0:em_taboo_len]
                    tmp_em_work.append(em_selected_add)
                    tmp_em_idle.remove(em_selected_add)
                    tmp_log.append([em_selected_add,'',zone_selected])
                elif    random_one<=random_index[1]:
                    # 替换

                    if em_selected_sub in tmp_strategy[zone_selected]:
                        tmp_strategy[zone_selected].remove(em_selected_sub)
                        tmp_strategy[zone_selected].append(em_selected_add)
                        tmp_zone_taboo.append(zone_selected)
                        tmp_zone_taboo = tmp_zone_taboo[0:zone_taboo_len]
                        tmp_em_taboo.append(em_selected_sub)
                        tmp_em_taboo.append(em_selected_add)
                        tmp_em_taboo = tmp_em_taboo[0:em_taboo_len]

                        tmp_em_work.append(em_selected_add)
                        tmp_em_idle.remove(em_selected_add)
                        tmp_em_work.remove(em_selected_sub)
                        tmp_em_idle.append(em_selected_sub)
                        tmp_log.append([em_selected_add, em_selected_sub,zone_selected])

                else:
                    # 删除
                    if em_selected_sub in tmp_strategy[zone_selected]:
                        tmp_strategy[zone_selected].remove(em_selected_sub)
                        tmp_zone_taboo.append(zone_selected)
                        tmp_zone_taboo = tmp_zone_taboo[0:zone_taboo_len]
                        tmp_em_taboo.append(em_selected_sub)
                        tmp_em_taboo = tmp_em_taboo[0:em_taboo_len]
                        tmp_em_work.remove(em_selected_sub)
                        tmp_em_idle.append(em_selected_sub)
                        tmp_log.append(['', em_selected_sub,zone_selected])


            # 更新

            obj_tmp = self.cost_json(day, tmp_strategy)

            if obj_tmp<=obj:

                strategy = tmp_strategy
                zone_taboo = tmp_zone_taboo
                em_taboo =  tmp_em_taboo
                obj = obj_tmp
                em_work = tmp_em_work
                em_idle =tmp_em_idle
                for k in tmp_log:
                    log.info('{},{} is operate into {}'.format(k[0],k[1],k[2]))


            gen+=once_gen

        return (strategy,obj)

    def check(self,solution):
        em = solution.values()
        em_work = []
        for i in em:
            em_work.extend(i)
        if len(em_work)!=len(set(em_work)):
            print('error  : repeatedly employee')
        else:
            print('no repeatedly employee')
    def optimize(self):
        best_strategy = defaultdict(dict)
        best_obj = defaultdict(dict)
        solution = self.read_dataStru_json()
        for day, strategy in solution.items():
            [best_strategy_tmp,obj] = self.algorithm1(day,strategy)
            best_strategy[day] = best_strategy_tmp
            best_obj[day] = obj
            self.check(best_strategy_tmp)
        with open(r"../results/best_strategy.json", "w") as fp:
            json.dump(best_strategy, fp)
        print(best_obj)




class Config(object):
    Dates = []
# load the data
filename = "1130_data_input_level_80cvar.xlsx"
data_ins = DataHandler(file=filename, config=Config)

mode = 'deterministic'
# target = 'reality'
target = 'predict'
level = '11_30_80var_80cvar_start'
# mode = 'expected'
# mode = 'expected_cvar'
solution_path = r"../results/solution_{}_{}_{}.json".format(mode, target, level)
optimization = Optimization(data_ins, solution_path)
optimization.optimize()

