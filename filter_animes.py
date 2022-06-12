import pandas as pd
import os

#print(os.listdir("data"))

intresting_years = ["2016", "2017", "2018", "2019", "2020"]
seasons = ["Fall ", "Winter ", "Spring ", "Summer "]

interesting_seasons = []
for y in range(len(intresting_years)):
    for s in range(len(seasons)):
        interesting_seasons +=[seasons[s] + intresting_years[y]]
print(interesting_seasons)

anime = pd.read_csv('data/anime.csv', sep = '\t', keep_default_na=False)
anime_count = 0
ids = []
titles = []
for i in range(len(anime)):
    #print(anime.loc[i, "season"])
    if(anime.loc[i, "season"]) in interesting_seasons:
        anime_count += 1
        ids += [anime.loc[i,"anime_id"]]
        titles+= [anime.loc[i,"title"]]
print(anime_count)

data = {'title': titles, 'id' : ids}
df = pd.DataFrame(data)
df.to_csv('data/filter_anime.csv')