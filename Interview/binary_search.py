# @Time : 2021/2/1 11:32 下午 
# @Author : Xingyou Chen
# @File : binary_search.py 
# @Software: PyCharm
li = [1,2,3,4,5,6]
def binary_search(li, left ,right, data):
    mid = (left + right) // 2
    while left <= right:
        if li[mid] < data:
            return binary_search(li, mid+1 , right, data)
        elif li[mid] > data:
            return binary_search(li, left, mid-1, data)
        else:
            return mid
    return -1
print(binary_search(li, 0 ,len(li)-1, 2))
print(binary_search(li, 0 ,len(li)-1, 7))