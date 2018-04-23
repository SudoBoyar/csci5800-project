import argparse
import os
import os.path
from subprocess import call
import sys


def main(args):
    for month in os.listdir(args.basedir):
        monthdir = os.path.join(args.basedir, month)
        out = os.path.join(args.outdir, month)
        if not os.path.exists(out):
            os.mkdir(out)

        if not os.path.isdir(monthdir):
            continue

        for day in os.listdir(monthdir):
            daydir = os.path.join(monthdir, day)
            if not os.path.isdir(daydir):
                continue

            for hour in os.listdir(daydir):
                hourdir = os.path.join(daydir, hour)
                if not os.path.isdir(hourdir):
                    continue

                outpath = os.path.join(out, '{}-{}-{}.json.bz2'.format(month, day, hour))
                if os.path.exists(outpath) and not args.force:
                    continue

                cat = ["cat"]
                for filename in os.listdir(hourdir):
                    ext = filename.split('.')[-1]
                    if ext == 'bz2':
                        cat.append(os.path.join(hourdir, filename))

                with open(outpath, 'w') as f:
                    call(cat, stdout=f)


def argument_parser(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('basedir', default='./2017')
    parser.add_argument('outdir', default='./processed')
    parser.add_argument('-f', dest='force', action='store_true', default=False)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = argument_parser(sys.argv[1:])
    main(args)
