# -*- coding: utf-8 -*-
"""
Yiwen Jiang

INLS 490 Project 2

04/2018
"""
from pandas import Series, DataFrame 
import pandas as pd
from numpy.random import randn 
import numpy as np

# Load data and then merge those two dataframes
u_a_df = pd.read_table('user_artists.dat', sep='\t')
a_df = pd.read_table('artists.dat', sep='\t')
u_a_merge = pd.merge(u_a_df, a_df, left_on='artistID', right_on='id')
u_f_df = pd.read_table('user_friends.dat', sep='\t')
a_t_df = pd.read_table('user_taggedartists.dat', sep='\t')

# function for Query 7
"""
Step 1. Use query to extract all the userID for each artist and store them into two sets;
Step 2. Use & and | to get the intersect and union of those to sets
Step 3. Use len() to get each set's size and then compute the Jaccard index 
"""
def artist_sim(aid1, aid2):
    print('\nartist_sim(', aid1,',', aid2,')')
    print('artist1:\t', a_df.name[a_df.id==aid1].values[0],
          '\nartist2:\t', a_df.name[a_df.id==aid2].values[0])
    user_set1 = set(u_a_df.query('artistID=='+str(aid1)).userID)
    user_set2 = set(u_a_df.query('artistID=='+str(aid2)).userID)
    intersect = user_set1 & user_set2
    union = user_set1 | user_set2
    J_index = len(intersect) / len(union)
    return J_index

print('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')

#------------------------------------------Query 1------------------------------------------------------------
"""
Create a GroupBy object grouped by artistID, then use sum() function to sum
the weight for each artist and sort the value, use filter to extract artist's
name from artists.data using artistID.
"""
print('1. Who are the top artists in terms of play counts?')
art_pc = u_a_merge['weight'].groupby(u_a_merge['artistID'])
a = art_pc.sum().sort_values(ascending = False)[:10]
for artid in a.index:
    print(a_df.name[a_df.id==artid].values[0]+"("+str(artid)+") "+str(a.get(artid)))

print('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')

#------------------------------------------Query 2------------------------------------------------------------
"""
Create a GroupBy object grouped by artistID, then use count() function to 
count the number of users listen to that artist and sort the value, use filter 
to extract artist's name from artists.data using artistID.
"""
print('2. What artists have the most listeners?')
art_ml = u_a_merge['userID'].groupby(u_a_merge['artistID'])
b = art_ml.count().sort_values(ascending = False)[:10]
for artid in b.index:
    print(a_df.name[a_df.id==artid].values[0]+"("+str(artid)+") "+str(b.get(artid)))

print('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')

#------------------------------------------Query 3------------------------------------------------------------
"""
Create a GroupBy object grouped by userID and artists' name, then use sum()
function to sum the weight for each user and sort the value.
"""
print('3. Who are the top users in terms of play counts?')
usr_pc = u_a_merge['weight'].groupby(u_a_merge['userID'])
c = usr_pc.sum().sort_values(ascending = False)[:10]
for usrid in c.index:
    print(str(usrid)+"\t"+str(c.get(usrid)))
    
print('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')

#------------------------------------------Query 4------------------------------------------------------------
"""
Step 1. Create a GroupBy object grouped by artistID and userID in terms of weight(art_avg), then use mean() 
        function to compute the average value of weight for each artist and sort the value, use level=0 as 
        the parameter of the mean() function to compute the mean value for each artistID.
Step 2. Use the GroupBy object art_avg, sum the total number of plays for each artist using level=0 
        and turn the result into a dataframe(e);
Step 3. Use the GroupBy object art_avg, count the number of different listeners for each artist and turn
        the result into a dataframe(f) and merge it with e. Now f's index would be artistID, and its columns
        are total number of plays and total number of listeners.
Step 4. Sort the average value, extract top10 and use .loc[] extract numbers from f.
        
"""
print('4. What artists have the highest average number of plays per listener?')
art_avg = u_a_merge.groupby(['artistID','userID'])['weight']
d = art_avg.sum().mean(level = 0).sort_values(ascending = False)[:10]
e = DataFrame(art_avg.sum().sum(level = 0).values, art_avg.sum().sum(level = 0).index)
e.columns = ['total'] 
f = DataFrame(art_avg.sum().count(level = 0).values, art_avg.sum().count(level = 0).index)
f.columns = ['num_l']
f = pd.merge(e, f, left_index = True, right_index = True)
for artid in d.index:
    print("\n"+a_df.name[a_df.id==artid].values[0]+"("+str(artid)+")\n\ttotal number of plays: "
          +str(f.loc[artid].values[0])+"\n\ttotal number of listeners: "
          +str(f.loc[artid].values[1])+"\n\taverage number: "+str(d.get(artid)))

print('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')

#------------------------------------------Query 5------------------------------------------------------------
"""
Step 1. Use the GroupBy object in Query4(art_avg), compute the avarage numer of plays for each artist, turn the 
        result into a dataframe(g) and merge it with f, at the same time filter artists with more than 50 
        listeners.
Step 2. Sort values by 'average' and get top10.
"""
print('5. What artists with at least 50 listeners have the highest average number of plays per listener?')
g = DataFrame(art_avg.sum().mean(level = 0).values, art_avg.sum().mean(level = 0).index)
g.columns = ['average']
h = pd.merge(g, f[f.num_l > 50], left_index = True, right_index = True)
h = h.sort_values(by='average', ascending=False)[:10]
for artid in h.index:
    print("\n"+a_df.name[a_df.id==artid].values[0]+"("+str(artid)+")\n\ttotal number of plays: "+
          str(h.loc[artid].values[1])+"\n\ttotal number of listeners: "+str(h.loc[artid].values[2])+
          "\n\taverage number: "+str(h.loc[artid].values[0]))

print('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')

#------------------------------------------Query 6------------------------------------------------------------
"""
Step 1. Create a GroupBy object grouped by userID in terms of friendID and count the total number of friends
        for each user and turn the result into a dataframe(usr_fn)
Step 2. Use the GroupBy object usr_pc in Query3, which counts the total number of plays for each user and turn 
        the result into a dataframe(i)
Step 3. Merge usr_fn and i, at the same time filter those users with 5 or 5 more friends(j) and those users 
        with less than 5 friends(k) and then get total number of plays for both two sets of users
Step 4. Use .iloc[:,0].size to get the total number of users in both two set of users, then compute the average
        number of plays.      
"""
print('6. Do users with five or more friends listen to more songs?')
usr_fn = u_f_df.groupby('userID')['friendID'].count()
usr_fn = DataFrame(usr_fn.values, usr_fn.index)
usr_fn.columns = ['num_f']
i = DataFrame(usr_pc.sum().values, usr_pc.sum().index)
i.columns = ['num_p']
j = pd.merge(usr_fn[usr_fn.num_f >= 5], i, left_index = True, right_index = True)
k = pd.merge(usr_fn[usr_fn.num_f < 5], i, left_index = True, right_index = True)
print('Average number of song plays for users with 5 or more than 5 friends:\n\t', 
      j.num_p.sum()/j.iloc[:,0].size)
print('Average number of song plays for users with less than 5 friends:\n\t', 
      k.num_p.sum()/k.iloc[:,0].size)
print('So the Answer is:\n\t Yes!!!')

print('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')

#------------------------------------------Query 7------------------------------------------------------------
print('7. How similar are two artists?')
print('Jaccard index:\t', artist_sim(735,562))
print('Jaccard index:\t', artist_sim(735,89))
print('Jaccard index:\t', artist_sim(735,289))
print('Jaccard index:\t', artist_sim(89,289))
print('Jaccard index:\t', artist_sim(89,67))
print('Jaccard index:\t', artist_sim(67,735))

print('\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')

#------------------------------------------Query 8------------------------------------------------------------
"""
Step 1. Create a GroupBy object grouped by artistID in terms of tagID and then use count()
        function to count overall number of tags for each artist and sort, extract the first 
        10 and get 10 artists with the highest overall number of tags;
Step 2. Create a GroupBy object grouped by month, year, and artistID in terms of tagID and
        then count the total number of tags for each artist in each date(month & year);
Step 3. Create another GroupBy to extract all different dates from this dataset(month & year);
Step 4. For each artist in the top10 with highest overall number of tags, for each date, find
        out whether it is in the top10 with highest overall number of tags in that date. If 
        in, then compare the date with previously detected date and keep the ealier one. At the
        same time, let the count plus 1, indicating it appears one more time in top10.
"""
print('8. Analysis of top tagged artists')
# top10 artists with the highest overall number of tags 
l = a_t_df.groupby('artistID')['tagID'].count().sort_values(ascending=False)[:10] 
m = a_t_df.groupby(['month','year','artistID'])['tagID'].count()
# extract all different date(month & year)
n = a_t_df.groupby(['month','year'])['tagID'].count() 
month_abbr = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
for artid in l.index:
    min_m = 0
    min_y = 0
    count = 0
    for i in range(0, len(n.index)):
        # iterate in all dates
        if artid in list(m[n.index[i][0]][n.index[i][1]].sort_values(ascending=False)[:10].index): 
            # if it is in top10
            count = count+1
            if min_m == 0 or n.index[i][1] < min_y or (n.index[i][1] == min_y and n.index[i][0] < min_m): 
                # if the date is earlier
                min_m = n.index[i][0]
                min_y = n.index[i][1]               
    print("\n"+a_df.name[a_df.id==artid].values[0]+"("+str(artid)+"): num tags = "+str(l[artid])
          +"\n\tfirst month in top10 = "+month_abbr[min_m-1]+" "+str(min_y)
          +"\n\tmonths in top10 = "+str(count))
                
                



