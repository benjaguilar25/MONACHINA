animes = LOAD 'hdfs://cm:9000/uhadoop2022/eldenring/filtered_anime.csv' USING PigStorage(',') AS (id,title,season);

popularity = LOAD 'hdfs://cm:9000/uhadoop2022/eldenring/popularity.csv' USING PigStorage('\t') AS (id, votes);

joined = JOIN animes BY id, popularity by id;

ordenados = ORDER joined by votes DESC;

grouped = GROUP ordenados by season;

bestOf = FOREACH grouped {
    top = TOP(1, 4, ordenados);
    GENERATE FLATTEN(top);
};

STORE joined INTO 'hdfs://cm:9000/uhadoop2022/eldenring/joined';
STORE ordenados INTO 'hdfs://cm:9000/uhadoop2022/eldenring/sorted';
STORE grouped INTO 'hdfs://cm:9000/uhadoop2022/eldenring/grouped';
STORE bestOf INTO 'hdfs://cm:9000/uhadoop2022/eldenring/besties';