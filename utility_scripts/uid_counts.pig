tweets = LOAD '/analyzed/*' USING PigStorage(';') AS (id:long, uid:long, user_followers:int, user_friends:int, user_favorited:int, user_status_count:int, user_tz:chararray, created_ts:long, tweet_rating:double, user_desc_rating:double, hashtags);
uids = FOREACH tweets GENERATE uid AS uid, 1 AS count;
groups = GROUP uids BY uid;
counts = FOREACH groups GENERATE group, SUM(uids.count) AS count;
ordered = ORDER counts BY count DESC;
STORE ordered INTO '/uid_counts' USING PigStorage(',', '-schema');