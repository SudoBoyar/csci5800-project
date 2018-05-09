tweets = LOAD '/analyzed/*' USING PigStorage(';', '-schema');
hashtags = FOREACH tweets GENERATE FLATTEN(hashtags) AS hashtag, 1 AS count;
groups = GROUP hashtags BY hashtag;
counts = FOREACH groups GENERATE group, SUM(hashtags.count) AS count;
filtered = FILTER counts BY count > 10;
ordered = ORDER filtered BY count DESC;
STORE ordered INTO '/hashtag_high_counts' USING PigStorage(',', '-schema');