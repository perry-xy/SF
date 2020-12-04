'''
# @Time        :2020/11/19 10:40
# @Author      :ChunRong.Chen
# @ FileName   :sensitivity_analysis.py
'''

from collections import  defaultdict
from  datetime import  datetime,timedelta, date
import json
import  pandas as pd
import matplotlib.pyplot as plt
import  numpy as np

plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False
REC_COST = 1
SEND_COST = 1
REC_DISCOUNT_COST = 18
SEND_DISCOUNT_COST = 12

class Sensitivity(object):
    def __init__(self, data_ins,solution_path):
        self.data_ins = data_ins
        self.solution_path = solution_path

    def read_DataStru_json(self):
        with open(self.solution_path, 'r', encoding='utf-8') as load_f:
            strF = load_f.read()
            if len(strF) > 0:
                datas = json.loads(strF)
            else:
                datas = {}
        return datas
    def Cost_json(self,op=None,delta = 0):

        cost_dict = defaultdict(int)
        daily_cost_dict = defaultdict(int)

        solution = self.read_DataStru_json()
        for day,strategy in solution.items():
            daily_cost = 0


            for zone,employees in strategy.items():
                demand_r,demand_s,cap_r,cap_s = 0,0,0,0
                cost_zone,basic_cost=0,0
                if isinstance(day, str):
                    day = datetime.strptime(day, '%Y-%m-%d')
                if not type(day) is date:
                    day = datetime.date(day)

                demand_r = self.data_ins.demands[day][zone]['receive']
                demand_s = self.data_ins.demands[day][zone]['send']
                if op == 'send':
                    demand_r = self.data_ins.demands[day][zone]['receive']+delta
                if op=='rec':
                    demand_s = self.data_ins.demands[day][zone]['send']+delta
                for employee in employees:
                    cap_r += self.data_ins.capacity[employee]['receive']
                    cap_s += self.data_ins.capacity[employee]['send']
                    basic_cost += self.data_ins.basic_cost[employee]
                real_r = min(cap_r,demand_r)
                real_s = min(cap_s,demand_s)
                dis_r = max(0,demand_r-cap_r)
                dis_s = max(0,demand_s-cap_s)
                cost_zone = (basic_cost                      #基础雇佣成本
                        +real_r*REC_COST                #收件成本
                        +real_s*SEND_COST               #派件成本
                        +dis_r*REC_DISCOUNT_COST        #收件惩罚成本
                        +dis_s*SEND_DISCOUNT_COST)      #派件惩罚成本
                daily_cost+=cost_zone
                cost_dict[zone] = cost_zone
            daily_cost_dict[day] = daily_cost
        return (daily_cost_dict)

    def disturb(self,mode,target,level):
        delta = list(range(-10,10))
        disturb_dict = defaultdict(dict)
        for i in delta:
            disturb_dict['send',i] = self.Cost_json('send',i)
            disturb_dict['rec',i] = self.Cost_json('rec', i)
        self.plot_disturb(disturb_dict,delta,mode,target,level)

    def plot_disturb(self,disturb_dict,delta, mode,target,level):

        x = delta
        y1_s = []
        y2_s = []
        y3_s = []
        y1_r = []
        y2_r = []
        y3_r = []
        ans = []

        for key,v in disturb_dict.items():
            for i,j in v.items():
                ans.append([key[0],key[1],i,j])
            val = list(v.values())
            legend = list(v.keys())
            a = plt.figure(num=1, figsize=(20, 10))
            if 'send' in key:
                y1_s.append(val[0])
                y2_s.append(val[1])
                y3_s.append(val[2])
            else:
                y1_r.append(val[0])
                y2_r.append(val[1])
                y3_r.append(val[2])

        # send
        a = plt.figure(num=1, figsize=(20, 10))
        title = 'send_distrub'
        plt.plot(x, y1_s, label=legend[0])
        plt.plot(x, y2_s, label=legend[1])
        plt.plot(x, y3_s, label=legend[2])
        plt.legend(fontsize=20)
        plt.xlabel('disturb', fontsize=15)
        plt.ylabel('成本', fontsize=15)
        plt.title(title)
        plt.savefig(title + '.jpg')


        # receive
        b = plt.figure(num=2, figsize=(20, 10))
        title = 'receive_distrub'
        plt.plot(x, y1_r, label=legend[0])
        plt.plot(x, y2_r, label=legend[1])
        plt.plot(x, y3_r, label=legend[2])
        plt.legend(fontsize=20)
        plt.xlabel('disturb', fontsize=15)
        plt.ylabel('成本', fontsize=15)
        plt.title(title)
        plt.savefig(title+'.jpg')
        per_excel = pd.DataFrame(data=np.array(ans).reshape(-1, 4),
                                 columns=['收/派','step','date','成本'])
        per_excel.to_excel('误差成本_{}_{}_{}.xlsx'.format(mode, target, level))



