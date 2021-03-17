# @Time : 2021/2/1 10:46 下午 
# @Author : Xingyou Chen
# @File : max_add_son_list.py 
# @Software: PyCharm

# 上升数组的最长公共子序列
def max_order(li):
    """
    动态规划：dp[i]为考虑li[:i+1]中，以li[i]为结尾的最长公共子序列
    状态转移方程：dp[j] = max(d[i]) + 1, i < j & li[j] > li[i]
    :param li:
    :return:
    """
    dp =[] # dp数组，第i个元素的含义为：在li[:i+1]中，以li[i]为结尾的最长公共子序列的长度
    for i in range(len(li)):
        dp.append(1)        #至少有自己
        for j in range(i):  #遍历之前的
            if li[i] > li[j]: #只考察比自己小的
                dp[i] = max(dp[i], dp[j] + 1)  #比自己小的，贡献自己肯定可以加1，但需要看一下有没有改善
    return max(dp)
print(max_order([2,4,10,6,9,8,7]))



