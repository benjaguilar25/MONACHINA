-- This script finds the actors/actresses with the highest number of good movies

raw_roles = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' USING PigStorage('\t') AS (star, title, year, num, type, episode, billing, char, gender);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' to see the full output

raw_ratings = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings.tsv' USING PigStorage('\t') AS (dist, votes, score, title, year, num, type, episode);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings.tsv' to see the full output

roles = FOREACH raw_roles GENERATE CONCAT(title,'##',year,'##',num) as movie, star, gender;
actors_roles = FILTER roles BY gender == 'MALE';
actresses_roles = FILTER roles BY gender == 'FEMALE';

movies = FILTER raw_ratings BY type == 'THEATRICAL_MOVIE';
movie_keys = FOREACH movies GENERATE CONCAT(title, '##', year, '##', num) as key, votes, score;

joined_roles_actor = JOIN actors_roles by movie, movie_keys by key;
joined_roles_actresses = JOIN actresses_roles by movie, movie_keys by key;

roles_ranked_actor =  FOREACH joined_roles_actor GENERATE star, (votes >= 10001 and score >= 7.8 ? 1 : 0) as rank;
roles_ranked_actresses =  FOREACH joined_roles_actresses GENERATE star, (votes >= 10001 and score >= 7.8 ? 1 : 0) as rank;

gruped_actors = GROUP roles_ranked_actor BY star;
gruped_actresses = GROUP roles_ranked_actresses BY star;

count_actors = FOREACH gruped_actors {
    good = FILTER $1 by rank==1;
    sum = COUNT(good);
    GENERATE group, sum as count;
    }
ordered_actors = ORDER count_actors BY $1 DESC;

count_actresses = FOREACH gruped_actresses {
    good = FILTER $1 by rank==1;
    sum = COUNT(good);
    GENERATE group, sum as count;
    }
ordered_actresses = ORDER count_actresses BY $1 DESC;

STORE count_actors INTO 'hdfs://cm:9000/uhadoop2022/eldenring/countActors';
STORE ordered_actors INTO 'hdfs://cm:9000/uhadoop2022/eldenring/topActors';

STORE ordered_actresses INTO 'hdfs://cm:9000/uhadoop2022/eldenring/topActresses';