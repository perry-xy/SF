# @Time : 2021/2/4 10:49 下午 
# @Author : Xingyou Chen
# @File : min_edit_dis.py 
# @Software: PyCharm
"""
给你两个单词word1和word2，请你计算出将word1转换成word2所使用的最少操作数。
你可以对一个单词进行如下三种操作：
插入一个字符
删除一个字符
替换一个字符
"""
def minEditdistance(word1, word2):
    """
    二维动态规划：dp[i][j]:将word1中前i个字符转换为word2中前j个字符所需要的最小步数
    若word1[i] = word2[j]：dp[i][j] = dp[i-1][j-1]
    若word1[i] != word2[j]：dp[i][j] = 1 + min(dp[i][j-1]（插入）,dp[i-1][j]（删除）,dp[i-1][j-1]（替换）
    :param word1:
    :param word2:
    :return:
    """
    n1, n2 = len(word1), len(word2)
    dp = [[0] * (n2 +1) for _ in range(n1+1)]
    # 第一行
    for i in range(1, n2+1):
        dp[0][i] = dp[0][i-1] + 1
    # 第一列
    for j in range(1, n1+1):
        dp[j][0] = dp[j-1][0] +1
    # 其他位置
    for i in range(1,n1+1):
        for j in range(1, n2+1):
            if word1[i-1] == word2[j-1]:
                dp[i][j] = dp[i-1][j-1]
            else:
                dp[i][j] = 1 + min(dp[i][j-1], dp[i-1][j], dp[i-1][j-1])
    return dp[-1][-1]
print(minEditdistance('horse', 'ros'))


