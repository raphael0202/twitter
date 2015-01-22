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

langList = list()
for row in languages :
    langList.append(row[0])

langList = Counter(langList).most_common()[0:5]

map = Basemap(projection='cyl', resolution=None,
              lat_0=0.,lon_0=0.)

map.bluemarble()

col = ['red','purple','yellow','blue','orange']

for l in languages:
    la = json.loads(l[1])
    #print v
    x,y = map(la[0],la[1])
    for i in range(0,5):
        if l[0] == langList[i][0] : map.plot(x,y,'o', markerfacecolor = col[i], markeredgecolor= 'none' ,alpha=.5, markersize=3)

patches=list()
for i in range(0,5):
    patches.append(mpatches.Patch(color=col[i], label=langList[i][0]))
    
plt.legend(handles=patches, loc=3, handlelength=1.5, labelspacing=0.2, fontsize=10)

fig = plt.gcf()
fig.set_size_inches(10,6)
fig.savefig('lang.png',dpi=100)
