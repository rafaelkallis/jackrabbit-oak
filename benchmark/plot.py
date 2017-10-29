#!/usr/bin/env python3

import matplotlib.pyplot as plt
from math import log
# from sys import argv
from ast import literal_eval
import numpy as np
import argparse
from os.path import isfile, basename
from glob import glob
from itertools import chain, repeat

parser = argparse.ArgumentParser()
parser.add_argument("glob", nargs="+")

parser.add_argument("--logx", action="store_true")
parser.add_argument("--logy", action="store_true")
parser.add_argument("--trend", action="store_true")

parser.add_argument("--xlabel", nargs="?", default="")
parser.add_argument("--ylabel", nargs="?", default="")
parser.add_argument("--title", nargs="?", default="")
parser.add_argument("--linetype", nargs="?", default="-")

args = parser.parse_args()

filepath = max(chain.from_iterable(map(glob, args.glob)))

print(filepath)

with open(filepath) as f:
    X = []
    Y = []
    for x, y in map(lambda line: literal_eval(line), filter(lambda line: line != "", f)):
        X.append(log(x + 1) if args.logx else x)
        Y.append(log(y + 1) if args.logy else y)
    plt.figure(basename(filepath))
    plt.xlabel(args.xlabel)
    plt.ylabel(args.ylabel)
    plt.title(args.title)
    plt.plot(X, Y, args.linetype)

    if args.trend:
        z = np.polyfit(X, Y, 1)
        p = np.poly1d(z)
        plt.plot(X, p(X), "r--")

    plt.show()
