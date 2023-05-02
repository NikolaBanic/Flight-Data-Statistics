# -*- coding: utf-8 -*-
"""
Created on Tue Jan 17 12:40:08 2023

@author: nikol
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv('airline_delay.csv')
df.pop('Unnamed: 0')

df.set_index('Airline company', inplace = True)



delay = pd.Series(df['Average delay true'])
delay_true = pd.Series(df['Average delay'])
nflights = pd.Series(df['Number of flights'])

fig,axes = plt.subplots(nrows = 1, ncols = 2, figsize = (9, 7), constrained_layout = True)

dff = df
dff.pop('Number of flights')

fig = dff.plot(kind ='bar', ax = axes[0], rot = 90)
fig.set_ylabel('Kašnjenje letova u minutama') 
fig.set_title('Kašnjenje letova')
fig = nflights.plot(kind = 'bar', ax = axes[1], rot = 90)
fig.set_title('Broj letova')
plt.suptitle('Analiza kašnjenja letova avionskih kompanija')

