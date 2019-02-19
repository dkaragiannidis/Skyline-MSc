#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Apr 20 13:48:50 2018

@author: apostol

Simple script to create multi-normal distributions.

"""

import matplotlib.pyplot as plt
import numpy as np

mean = [5, 0.2]
cov = [[7, 0], [0.5, 7]]
x1, y1 = np.random.multivariate_normal(mean, cov, 500).T
#plt.plot(x1, y1, '.')

mean = [10, 8]
cov = [[4, 2], [0.5, 5]]
x2, y2 = np.random.multivariate_normal(mean, cov, 1000).T
#plt.plot(x2, y2, '.')

mean = [10, 10]
cov = [[4, 2], [0.5, 4]]
x3, y3 = np.random.multivariate_normal(mean, cov, 2000).T
#plt.plot(x, y, '.')

mean = [20, -10]
cov = [[2, 0], [0.2, 0.9]]
x4, y4 = np.random.multivariate_normal(mean, cov, 500).T
#plt.plot(x, y, '.')

mean = [-10, 15]
cov = [[0.5, 0.2], [0.1, 0.8]]
x5, y5 = np.random.multivariate_normal(mean, cov, 6000).T
#plt.plot(x, y, '.')

xl = x1.tolist() + x2.tolist() + x3.tolist() + x4.tolist() + x5.tolist()
yl = y1.tolist() + y2.tolist() + y3.tolist() + y4.tolist() + y5.tolist()


'''
#find min and max in all dimensions
min_x
max_x

min_y
max_y

'''

min_x = min(xl)
max_x = max(xl)
min_y = min(yl)
max_y = max(yl)

for i,e in enumerate(xl):
    xl[i] = (xl[i] - min_x)/(max_x - min_x)
    if xl[i] < 0:
        xl[i] = 0
    if xl[i] > 1.0:
        xl[i] = 1.0
    
for i,e in enumerate(yl):    
    yl[i] = (yl[i] - min_y)/(max_y - min_y)
    if yl[i] < 0:
        yl[i] = 0
    if yl[i] > 1.0:
        yl[i] = 1.0

# plot the distribution
plt.plot(xl, yl, 'o', mfc='none', color='blue')
axes = plt.gca()
axes.set_xlim([0,1])
axes.set_ylim([0,1])
plt.savefig('mg.eps', format='eps')
plt.show()

# save the dataset to a flie

f = open("out.txt", "w")

for i in range(0,len(xl)):
    string = str(xl[i]) + " " + str(yl[i]) + "\n"
    f.write(string)

f.close()

    


