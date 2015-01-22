# -*- coding: utf-8 -*-
"""
Created on Thu Jan 22 17:32:59 2015

@author: tiffany
"""

import sqlite3
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
import json
from collections import Counter
import matplotlib.patches as mpatches


conn = sqlite3.connect("tweets.db")
c = conn.cursor()
languages = c.execute("SELECT LANG, COORDINATES FROM TWEET,PLACE WHERE TWEET.PLACE_ID = PLACE.PLACE_ID").fetchall()

# map = Basemap(projection='merc',llcrnrlat=-80,urcrnrlat=80,\
#            llcrnrlon=-180,urcrnrlon=180,lat_ts=20,resolution='c')

              

langList = [('fr',),('en',),('es',)]

france = Basemap(llcrnrlon=-7.,llcrnrlat=41,urcrnrlon=12,urcrnrlat=51.5, resolution='i')


france.bluemarble()

col = ['yellow','red', 'orange']

for l in languages:
    la = json.loads(l[1])
    #print v
    x,y = france(la[0],la[1])
    for i in range(0,len(langList)):
        if l[0] == langList[i][0] : france.plot(x,y,'o', markerfacecolor = col[i], markeredgecolor= 'none' ,alpha=.5, markersize=5)

patches=list()


for i in range(0,len(langList)):
    patches.append(mpatches.Patch(color=col[i], label=langList[i][0]))
    
plt.legend(handles=patches, loc=3, handlelength=1.5, labelspacing=0.2, fontsize=10)

fig = plt.gcf()
fig.set_size_inches(10,10)
fig.savefig('locallang.png',dpi=100)
