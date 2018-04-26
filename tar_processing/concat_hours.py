import argparse
from math import floor
import os
import os.path
from subprocess import call
import sys

pig_template = """
REGISTER '/home/s_alex_klein/elephant-bird-hadoop-compat-4.1.jar';

REGISTER '/home/s_alex_klein/elephant-bird-pig-4.1.jar';
 
REGISTER '/home/s_alex_klein/json-simple-1.1.1.jar';

load_tweets = LOAD '/input/{month}/g{group}' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS myMap;

english = FILTER load_tweets BY myMap#'lang' == 'en';

details = FOREACH english GENERATE myMap#'id' AS id, myMap#'timestamp_ms' AS created_ts, (myMap#'truncated' == 'true' ? myMap#'extended_tweet'#'full_text' : myMap#'text') AS text, (myMap#'truncated' == 'true' ? myMap#'extended_tweet'#'entities'#'hashtags' : myMap#'entities'#'hashtags') AS hashtag_list, myMap#'user'#'id' AS uid, myMap#'user'#'description' AS user_desc, myMap#'user'#'followers_count' AS user_followers, myMap#'user'#'friends_count' AS user_friends, myMap#'user'#'favourites_count' AS user_favorited, myMap#'user'#'statuses_count' AS user_status_count, myMap#'user'#'time_zone' AS user_tz;


hashtag_data = FOREACH details GENERATE id, FLATTEN(hashtag_list) AS hashtag_info;
hashtags = FOREACH hashtag_data GENERATE id, hashtag_info#'text' AS hashtag;
hashtag_grouping = GROUP hashtags BY id;
hashtag_groups = FOREACH hashtag_grouping GENERATE group AS id, hashtags.hashtag AS hashtags;

tweet_hashtags = JOIN details BY id LEFT OUTER, hashtag_groups BY id USING 'replicated' PARALLEL 4;

stripped_tweets = FOREACH tweet_hashtags GENERATE details::id AS id, details::uid AS uid, details::user_followers AS user_followers, details::user_friends AS user_friends, details::user_favorited AS user_favorited, details::user_status_count AS user_status_count, details::user_tz AS user_tz, details::created_ts AS created_ts, details::user_desc AS user_desc, details::text AS text, hashtag_groups::hashtags AS hashtags;

STORE stripped_tweets INTO '/processed/{monthname}{group}.txt' USING PigStorage('\\\\x0B', '-schema');
"""

pig_filename_template = '{monthname}{group}.pig'

runall_command_template = """#!/bin/bash

{commands}
"""

run_command_template = "pig {pig_filename}"

runall_filename_template = 'run_{monthname}.sh'

months = {
    1: 'january',
    2: 'february',
    3: 'march',
    4: 'april',
    5: 'may',
    6: 'june',
    7: 'july',
    8: 'august',
    9: 'september',
    10: 'october',
    11: 'november',
    12: 'december'
}


def main(args):
    global pig_template
    global pig_filename_template
    global runall_command_template
    global runall_filename_template
    global run_command_template
    global months


    for month in os.listdir(args.basedir):
        monthdir = os.path.join(args.basedir, month)
        out = os.path.join(args.outdir, month)
        if not os.path.exists(out):
            os.mkdir(out)

        if not os.path.isdir(monthdir):
            continue

        groups = set()

        for day in os.listdir(monthdir):
            print("Day", day)
            daydir = os.path.join(monthdir, day)
            if not os.path.isdir(daydir):
                continue

            group = floor(int(day)/3.0)
            outsubdir = 'g' + str(group)
            tmp = os.path.join(out, outsubdir)
            if not os.path.exists(tmp):
                os.mkdir(tmp)

            for hour in os.listdir(daydir):
                print("Hour", hour)
                hourdir = os.path.join(daydir, hour)
                if not os.path.isdir(hourdir):
                    continue

                outpath = os.sep.join([out, outsubdir, '{}-{}-{}.json.bz2'.format(month, day, hour)])
                if os.path.exists(outpath) and not args.force:
                    groups.add(group)
                    continue

                cat = ["cat"]
                for filename in os.listdir(hourdir):
                    ext = filename.split('.')[-1]
                    if ext == 'bz2':
                        cat.append(os.path.join(hourdir, filename))

                if len(cat) == 1:
                    continue

                groups.add(group)
                with open(outpath, 'w') as f:
                    call(cat, stdout=f)

        monthname = months[int(month)]
        run_cmds = []
        for group in groups:
            pigfile = pig_filename_template.format(monthname=monthname, group=group)
            pig_contents = pig_template.format(month=int(month), monthname=monthname, group=group)

            with open(os.path.join(args.outdir, pigfile), 'w') as f:
                f.write(pig_contents)
            run_cmds.append(run_command_template.format(pig_filename=pigfile))

        commands = ' && '.join(run_cmds)
        runall_file = runall_filename_template.format(monthname=monthname)
        runall_contents = runall_command_template.format(commands=commands)

        with open(os.path.join(args.outdir, runall_file), 'w') as f:
            f.write(runall_contents)


def argument_parser(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('basedir', default='./2017')
    parser.add_argument('outdir', default='./processed')
    parser.add_argument('-f', dest='force', action='store_true', default=False)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = argument_parser(sys.argv[1:])
    main(args)
