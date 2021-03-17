# @Time : 2021/2/23 10:01 下午 
# @Author : Xingyou Chen
# @File : twonumbers_sum.py 
# @Software: PyCharm
def twonumbersum(li, target):
    map = dict()
    for i , num in enumerate(li):
        if target - num in map:
            return([i, map[target-num]])
        map[num] = i
    return []

print(twonumbersum([1,2,3,4],7))