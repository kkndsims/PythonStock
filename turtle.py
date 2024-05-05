# -*- coding: utf-8 -*-
"""
Created on Mon Dec 25 20:37:53 2023

@author: sims

"""

import turtle as t

if __name__ == '__main__' :
    colors = ['red', 'purple', 'blue', 'green', 'yellow', 'orange']
    t.color('red')
    print("开始画图：数字1带颜色，其他数字不带颜色")
    t.bgcolor('black')
    
    draw = True
    draw = False
    draw = int(input())

    if draw == 1:
        for i in range(60):
            t.pencolor(colors[i%6])
            t.width(i/100+1)
            t.forward(i)
            t.left(59)
        t.done()
    else:
        for i in range(60):
            #print(i)
            t.fd(i)
            t.left(70)
        t.done()
    