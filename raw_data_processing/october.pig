REGISTER '/home/s_alex_klein/elephant-bird-hadoop-compat-4.1.jar';

REGISTER '/home/s_alex_klein/elephant-bird-pig-4.1.jar';
 
REGISTER '/home/s_alex_klein/json-simple-1.1.1.jar';

load_tweets = LOAD '/input/10/' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS myMap;

english = FILTER load_tweets BY myMap#'lang' == 'en';

details = FOREACH english GENERATE myMap#'id' AS id, myMap#'timestamp_ms' AS created_ts, (myMap#'truncated' == 'true' ? myMap#'extended_tweet'#'full_text' : myMap#'text') AS text, (myMap#'truncated' == 'true' ? myMap#'extended_tweet'#'entities'#'hashtags' : myMap#'entities'#'hashtags') AS hashtag_list, myMap#'user'#'id' AS uid, myMap#'user'#'description' AS user_desc, myMap#'user'#'followers_count' AS user_followers, myMap#'user'#'friends_count' AS user_friends, myMap#'user'#'favourites_count' AS user_favorited, myMap#'user'#'statuses_count' AS user_status_count, myMap#'user'#'time_zone' AS user_tz;


hashtag_data = FOREACH details GENERATE id, FLATTEN(hashtag_list) AS hashtag_info;
hashtags = FOREACH hashtag_data GENERATE id, hashtag_info#'text' AS hashtag;
hashtag_grouping = GROUP hashtags BY id;
hashtag_groups = FOREACH hashtag_grouping GENERATE group AS id, hashtags.hashtag AS hashtags;

tweet_hashtags = JOIN details BY id LEFT OUTER, hashtag_groups BY id USING 'replicated' PARALLEL 4;

stripped_tweets = FOREACH tweet_hashtags GENERATE details::id AS id, details::uid AS uid, details::user_followers AS user_followers, details::user_friends AS user_friends, details::user_favorited AS user_favorited, details::user_status_count AS user_status_count, details::user_tz AS user_tz, details::created_ts AS created_ts, details::user_desc AS user_desc, details::text AS text, hashtag_groups::hashtags AS hashtags;

STORE stripped_tweets INTO '/processed/october.txt' USING PigStorage('\\x0B', '-schema');