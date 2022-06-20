-- This script finds the actors/actresses with the highest number of good movies

raw_roles = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars-test-g8.tsv' USING PigStorage('\t') AS (star, title, year, num, type, episode, billing, char, gender);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-stars.tsv' to see the full output

raw_ratings = LOAD 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings-test-g8.tsv' USING PigStorage('\t') AS (dist, votes, score, title, year, num, type, episode);
-- Later you can change the above file to 'hdfs://cm:9000/uhadoop/shared/imdb/imdb-ratings.tsv' to see the full output

roles = FOREACH raw_roles GENERATE CONCAT(title,'##',year,'##',num) as movie, star, gender;
actors_roles = FILTER roles BY gender == 'MALE';
actresses_roles = FILTER roles BY gender == 'FEMALE';

movies = FILTER raw_ratings BY type == 'THEATRICAL_MOVIE';
movie_keys = FOREACH movies GENERATE CONCAT(title, '##', year, '##', num) as key, votes, score;

joined_roles_actor = JOIN actors_roles by movie, movie_keys by key;
-- joined_roles_actresses = JOIN actresses_roles by movie, movie_keys by key;

roles_ranked_actor =  FOREACH joined_roles_actor GENERATE star, (votes >= 10001 and score >= 7.8 ? 1 : 0) as good;
gruped_actors = GROUP roles_ranked_actor BY star;
summed_count_actors = FOREACH gruped_actors GENERATE group, SUM(gruped_actors) as count;
ordered_good_actors = ORDER summed_count_actors BY count DESC;

STORE roles_ranked_actor_mierderos INTO 'hdfs://cm:9000/uhadoop2022/eldenring/intento1';

-- roles_ranked_actresses =  FOREACH joined_roles_actresses GENERATE star, (votes >= 10001 and score >= 7.8 ? 1 : 0);
--gruped_actresses = GROUP roles_ranked_actresses BY star;
--summed_count_actresses = FOREACH gruped_actresses GENERATE $0, SUM($1) as count;
--ordered_good_actresses = ORDER summed_count_actresses BY count DESC;

--STORE ordered_good_actresses INTO 'hdfs://cm:9000/uhadoop2022/eldenring/cheetotestW';