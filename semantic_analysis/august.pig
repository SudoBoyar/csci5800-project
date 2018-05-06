dictionary = LOAD '/AFINN.txt' USING PigStorage('\t') AS(word:chararray,rating:int);

tweets = LOAD '/processed/august/*.txt' USING PigStorage('\\x0B', '-schema');

flat_tweets = FOREACH tweets GENERATE id, FLATTEN(TOKENIZE(REPLACE(LOWER(TRIM(text)), '[\\p{Punct},\\p{Cntrl}]',''))) AS word;
flat_user_desc = FOREACH tweets GENERATE id, FLATTEN(TOKENIZE(REPLACE(LOWER(TRIM(user_desc)), '[\\p{Punct},\\p{Cntrl}]',''))) AS word;

tweets_no_text = FOREACH tweets GENERATE id, uid, user_followers, user_friends, user_favorited, user_status_count, user_tz, created_ts, hashtags;

tweet_word_rating = JOIN flat_tweets BY word, dictionary BY word USING 'replicated';
user_desc_rating = JOIN flat_user_desc BY word, dictionary BY word USING 'replicated';

tweet_rating = FOREACH tweet_word_rating GENERATE flat_tweets::id AS id, dictionary::rating AS rate;
desc_rating = FOREACH user_desc_rating GENERATE flat_user_desc::id AS id, dictionary::rating AS rate;

tweet_word_group = GROUP tweet_rating BY id;
desc_word_group = GROUP desc_rating BY id;

avg_tweet_rate = FOREACH tweet_word_group GENERATE group AS id, AVG(tweet_rating.rate) AS tweet_rating;
user_desc_rate = FOREACH desc_word_group GENERATE group AS id, AVG(desc_rating.rate) AS desc_rating;

joined_tweets = JOIN tweets_no_text BY id, avg_tweet_rate BY id PARALLEL 4;
joined = JOIN joined_tweets BY tweets_no_text::id LEFT OUTER, user_desc_rate BY id PARALLEL 4;

result = FOREACH joined GENERATE joined_tweets::tweets_no_text::id AS id, joined_tweets::tweets_no_text::uid AS uid, joined_tweets::tweets_no_text::user_followers AS user_followers, joined_tweets::tweets_no_text::user_friends AS user_friends, joined_tweets::tweets_no_text::user_favorited AS user_favorited, joined_tweets::tweets_no_text::user_status_count AS user_status_count, joined_tweets::tweets_no_text::user_tz AS user_tz, joined_tweets::tweets_no_text::created_ts AS created_ts, joined_tweets::avg_tweet_rate::tweet_rating AS tweet_rating, user_desc_rate::desc_rating AS user_desc_rating, joined_tweets::tweets_no_text::hashtags AS hashtags;

wellformed = FILTER result BY id MATCHES '\\d{18}';

STORE wellformed INTO '/analyzed/august' USING PigStorage(';', '-schema');
