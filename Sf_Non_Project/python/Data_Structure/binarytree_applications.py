#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 16 17:21:26 2021

@author: perry
"""

from pythonds_basic import Stack
from pythonds_basic import BinaryTree
import operator

def buildParseTree(fpexp):
    
    fplist = list(fpexp)
    pStack = Stack()
    eTree = BinaryTree('')
    pStack.push(eTree)
    currentTree = eTree
    
    for i in fplist:
        if i == '(':
            currentTree.insertLeft('')
            pStack.push(currentTree)
            y = pStack.peek()
            currentTree = currentTree.getLeftChild()
        elif i not in '+-*/)':
            currentTree.setRootVal(eval(i))
            parent = pStack.pop()
            currentTree = parent
        elif i in '+-*/':
            currentTree.setRootVal(i)
            currentTree.insertRight('')
            pStack.push(currentTree)
            currentTree = currentTree.getRightChild()
        elif i ==')':
            currentTree = pStack.pop()
        else:
            raise ValueError('Unknown Operator: ' + i)
    
    return eTree

x = buildParseTree("(3+(4*5))")

def evaluate(parseTree):
    
    opers = {'+':operator.add, '-':operator.sub,
             '*':operator.mul, '/':operator.truediv}
    leftC = parseTree.getLeftChild()
    rightC = parseTree.getRightChild()
    
    if leftC and rightC:
        fn = opers[parseTree.getRootVal()]
        return fn(evaluate(leftC), evaluate(rightC))
    
    else:
        return parseTree.getRootVal()

y = evaluate(x)
        
