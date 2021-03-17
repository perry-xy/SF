# @Time : 2021/2/1 11:42 下午 
# @Author : Xingyou Chen
# @File : linklist_postorder.py 
# @Software: PyCharm
class linknode():
    def __init__(self, value):
        self.data = value
        self.next = None

def create_link(li):
    head = linknode(li[0])
    now = head
    for i in li[1:]:
        now.next = linknode(i)
        now = now.next  #最后一个的尾指针为None
    return head

def print_link(head):
    """
    :param head: 头结点，是一个链表对象
    :return:
    """
    if head == None:
        return None
    else:
        node = head
        while node != None:
            print(node.data)
            node = node.next
li = [3,4,5,1,2]
head = create_link(li)
print_link(head)

def reverseLink(head):
    """
    思路：从头处理至尾，①先将存当前节点的下一个节点暂存，②再将当前节点的下一个节点指向上一个处理过的节点，③把当前节点作为处理过的，④把暂存的变为当前的
    需要变量：pre：处理过的，cur：当前的
    :param head:
    :return:
    """
    pre = None   # 刚处理过的节点
    cur = head   # 待处理节点
    while cur:   # 尾节点的next为None
        tmp = cur.next  # 记住当前节点的下一个节点
        cur.next = pre  # 将当前节点的下一个节点指向上一个处理过的节点
        pre = cur       # 将当前节点变为处理过的节点
        cur = tmp       # 将当前节点的下一个节点变为待处理节点
    return pre

reverse = reverseLink(head)
print_link(reverse)





