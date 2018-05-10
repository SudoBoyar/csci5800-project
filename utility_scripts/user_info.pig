tweets = LOAD '/analyzed/*' USING PigStorage(';') AS (id:long, uid:long, user_followers:int, user_friends:int, user_favorited:int, user_status_count:int, user_tz:chararray, created_ts:long, tweet_rating:double, user_desc_rating:double, hashtags:{(hashtag:chararray)});

tzreg = LOAD '/timezone_region.csv' USING PigStorage(',') AS(tz:chararray,region_id:int);

tweetreg = JOIN tweets BY user_tz, tzreg BY tz USING 'replicated';

user_info = FOREACH tweetreg GENERATE tweets::uid AS uid, tweets::user_followers AS followers, tweets::user_friends AS friends, tweets::user_favorited AS favorited, tweets::user_status_count AS status_count, tzreg::region_id AS region_id, tweets::tweet_rating AS tweet_rating, tweets::user_desc_rating AS user_desc_rating, 1 AS count;

user_info_grp = GROUP user_info BY uid PARALLEL 16;

user_info_avg = FOREACH user_info_grp GENERATE group AS uid, AVG(user_info.followers) AS followers, AVG(user_info.friends) AS friends, AVG(user_info.favorited) AS favorited, AVG(user_info.status_count) AS status_count, MIN(user_info.region_id) AS region_id, AVG(user_info.tweet_rating) AS tweet_rating, AVG(user_info.user_desc_rating) AS user_desc_rating, SUM(user_info.count) AS count;


STORE user_info_avg INTO '/user_info' USING PigStorage(',', '-schema');