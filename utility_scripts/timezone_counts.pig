tweets = LOAD '/analyzed/*' USING PigStorage(';', '-schema');

tzs = FOREACH tweets GENERATE user_tz AS tz, 1 AS tzcount;

tz_group = GROUP tzs BY tz;

tzcounts = FOREACH tz_group GENERATE group AS tz, SUM(tzs.tzcount) AS cnt;

STORE tzcounts INTO '/tzcounts' USING PigStorage(';', '-schema');