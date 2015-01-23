import sqlite3
import matplotlib.pyplot as plt
import json
%matplotlib inline

#idea : take some tweet. Plot fraction of tweets using the same language depending on distance

conn = sqlite3.connect("tweets.db") ###REPLACE tweets.db BY THE DATABASE WE WILL GET
c = conn.cursor()

base = c.execute("SELECT COORDINATES, LANG " 
                 "FROM TWEET, PLACE "
                 "WHERE TWEET.PLACE_ID = PLACE.PLACE_ID").fetchall()

def distance_language_plot(initial_point):
    ref_lang=initial_point[1]
    ref_coords=json.loads(initial_point[0])

    new_base=dict()
    for item in base:
        coords=json.loads(item[0])
        lang=item[1]
        distance=np.arccos(np.sin(coords[0])*np.sin(ref_coords[0])+np.cos(coords[0])*np.cos(ref_coords[0])*np.cos(ref_coords[1]-coords[1]))*6378
        if distance not in new_base:
            new_base[distance]={lang : 1}
        else:
            if lang in new_base[distance]:
                new_base[distance][lang]+=1
            else:
                new_base[distance][lang]=1
    distance_list=new_base.keys()
    distance_list.sort()
    number_of_tweets=[] #contains elements of form (number with same language, total number)
    same_lang=0
    total_lang =0

    for distance in distance_list:
        data_for_distance=new_base[distance]
        same_lang_at_distance=0
        total_lang_at_distance=0
        for lang in data_for_distance:
            if lang == ref_lang:
                same_lang_at_distance+=data_for_distance[lang]
            total_lang_at_distance+=data_for_distance[lang]
        same_lang+=same_lang_at_distance
        total_lang+=total_lang_at_distance
        number_of_tweets.append(float(same_lang)/total_lang)
        #note : if we have enough points, it might be good to see what same_lang_at_distance/total_lang gives
        #or if not enough points have the same distance, maybe aggregate close points ?
    plt.plot(distance_list,number_of_tweets)
    plt.show()
