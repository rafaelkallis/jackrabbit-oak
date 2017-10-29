#!/usr/bin/env python3

import matplotlib.pyplot as plt
from math import log
from ast import literal_eval
import numpy as np
import argparse
from os.path import isfile, basename
from glob import glob
from itertools import chain, repeat

parser = argparse.ArgumentParser()
parser.add_argument("glob", nargs="+")

#parser.add_argument("--logx", action="store_true")
#parser.add_argument("--logy", action="store_true")
parser.add_argument("--scatter", action="store_true")
parser.add_argument("--seconds", action="store_true")
#parser.add_argument("--trend", action="store_true")
parser.add_argument("--minutes", action="store_true")
parser.add_argument("--convolve", action="store_true")

parser.add_argument("--xlabel", nargs="?", default="")
parser.add_argument("--ylabel", nargs="?", default="")
parser.add_argument("--title", nargs="?", default="")
#parser.add_argument("--linetype", nargs="?", default="-")

args = parser.parse_args()

plt.xlabel(args.xlabel)
plt.ylabel(args.ylabel)
plt.title(args.title)

for filepath in chain.from_iterable(map(glob, args.glob)):
    with open(filepath) as f:
        X, Y = zip(*[literal_eval(l) for l in f])
        if args.seconds:
            X = np.array(X)/1000
        if args.minutes:
            X = np.array(X)/60000
        #if args.logx:
        #    X = [log(x + 1) for x in X]
        #if args.logy:
        #    Y = [log(y + 1) for y in Y]
        if args.convolve:
            Y = np.convolve(Y, np.ones(10)/10, "same")
        
        if args.scatter:
            plt.scatter(X, Y, s=1)
        else:
            plt.plot(X, Y)

        #if args.trend:
        #    z = np.polyfit(X, Y, 1)
        #    p = np.poly1d(z)
        #    plt.plot(X, p(X), "--")

plt.show()
