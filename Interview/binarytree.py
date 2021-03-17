# @Time : 2021/2/5 12:13 上午 
# @Author : Xingyou Chen
# @File : binarytree.py 
# @Software: PyCharm
class BinaryTree():
    def __init__(self, value):
        self.value = value
        self.left = None
        self.right = None

def re_postorder(root):
    if root is None:
        return
    print(root.value)
    re_postorder(root.left)
    re_postorder(root.right)

def re_inorder(root):
    if not root:
        return
    re_inorder(root.left)
    print(root.left)
    re_inorder(root.right)

def re_postorder(root):
    if not root:
        return
    re_postorder(root.left)
    re_postorder(root.right)
    print(root.value)

def pre_order(root):
    white, gray = 0, 1
    stack = [(white, root)]
    while stack:
        color, node = stack.pop()
        if not node:
            continue
        if color == white:
            stack.append((white, node.right))
            stack.append((white, node.left))
            stack.append((gray, node))
        else:
            print(node.value)

