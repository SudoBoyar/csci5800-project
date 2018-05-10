tweets = LOAD '/analyzed/*' USING PigStorage(';') AS (id:long, uid:long, user_followers:int, user_friends:int, user_favorited:int, user_status_count:int, user_tz:chararray, created_ts:long, tweet_rating:double, user_desc_rating:double, hashtags:{(hashtag:chararray)});

regions = LOAD '/regions.csv' USING PigStorage(',') AS(id:int,name:chararray);
tzreg = LOAD '/timezone_region.csv' USING PigStorage(',') AS(tz:chararray,region_id:int);

tweetreg = JOIN tweets BY user_tz, tzreg BY tz USING 'replicated' PARALLEL 8;

perhashtag = FOREACH tweetreg GENERATE tweets::id AS id, GetMonth(ToDate(tweets::created_ts)) AS month, tzreg::region_id AS region_id, tweets::tweet_rating AS tweet_rating, FLATTEN(tweets::hashtags) AS hashtag, 1 AS count;

perhashtag = FOREACH perhashtag GENERATE id, month, region_id, tweet_rating, LOWER(hashtag) AS hashtag, count;

groups = GROUP perhashtag BY (hashtag, month, region_id) PARALLEL 8;

month_avg = FOREACH groups GENERATE group.hashtag AS hashtag, group.month AS month, group.region_id AS region_id, AVG(perhashtag.tweet_rating) AS rating, SUM(perhashtag.count) AS count;

avg_regions = JOIN month_avg BY region_id, regions BY id USING 'replicated' PARALLEL 8;

result = FOREACH avg_regions GENERATE month_avg::hashtag AS hashtag, month_avg::month AS month, month_avg::region_id AS region_id, regions::name AS region_name, month_avg::rating AS rating, month_avg::count AS count;

STORE result INTO '/monthly_hashtag_sentiments' USING PigStorage(',', '-schema');