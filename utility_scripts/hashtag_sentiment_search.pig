sentiments = LOAD '/monthly_hashtag_sentiments' USING PigStorage(',', '-schema');

filtered = FILTER sentiments BY hashtag == '$hashtag';

result = FOREACH filtered GENERATE hashtag, month, region_name, rating, count;

dump ordered;