# @Time : 2021/2/23 9:32 下午 
# @Author : Xingyou Chen
# @File : longestsubstring.py 
# @Software: PyCharm

def longestsubstr(str):
    """
    找出最长上升子串
    滑动窗口：依次滑动，当新进入的已经在时，依次把左边的剔除，直至新进入的不在里面（只要有重复的，就需要从当前这一个开始选新的）
    :param str:
    :return:
    """
    n = len(str)
    cur_len = 0
    max_len = 0
    lookup = list()
    left = 0
    for i in range(n):
        cur_len += 1
        while str[i] in lookup:
            lookup.remove(str[left])
            cur_len -= 1
            left += 1
        if max_len < cur_len:
            max_len = cur_len
        lookup.append(str[i])

    return max_len
print(longestsubstr("abcabcbb"))