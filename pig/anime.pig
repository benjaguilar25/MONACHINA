animes = LOAD 'hdfs://cm:9000/uhadoop2022/eldenring/filtered_anime.csv' USING PigStorage(',') AS (id:int,title,season);

popularity = LOAD 'hdfs://cm:9000/uhadoop2022/eldenring/popularity.csv' USING PigStorage('\t') AS (id:int, votes:int);

filtered1 = FILTER animes BY (id is not null);

filtered0 = FILTER filtered1 BY (title is not null);

filtered = FILTER filtered0 BY (season is not null);

joined = JOIN filtered BY id, popularity by id;

grouped = GROUP joined BY season;

bestof = FOREACH grouped {
    sort = ORDER joined BY popularity::votes DESC;
    best = LIMIT sort 3;
    GENERATE FLATTEN(best);
};

STORE joined INTO 'hdfs://cm:9000/uhadoop2022/eldenring/joined';
STORE bestof INTO 'hdfs://cm:9000/uhadoop2022/eldenring/best-animes' USING PigStorage (',');
