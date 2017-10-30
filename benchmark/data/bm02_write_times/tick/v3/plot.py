#!/usr/bin/env python3

from glob import glob
from ast import literal_eval
import matplotlib.pyplot as plt
from itertools import islice
from statistics import mean, median

def partition(l, n):
    buf = []
    for item in l:
        buf.append(item)
        if (len(buf) == n):
            yield buf
            buf = []

filenames = sorted(list(glob("output_write_time*")))

#

fig = plt.figure()
for filename in filenames:
    with open(filename) as f:
        X, Y = zip(*map(literal_eval, f))
        plt.scatter(list(X), list(Y), s=1)

plt.legend(("τ = 0","τ = 5","τ = 10"))
plt.xlabel("Tick")
plt.ylabel("Update Execution Runtime [ms]")
fig.savefig("figure_all.png")

#

fig = plt.figure()
for filename in filenames:
    with open(filename) as f:
        X, Y = zip(*map(literal_eval, f))
        X = map(mean, partition(X, 10))
        Y = map(mean, partition(Y, 10))
        plt.scatter(list(X), list(Y), s=1)

plt.legend(("τ = 0","τ = 5","τ = 10"))
plt.xlabel("Tick")
plt.ylabel("Update Execution Runtime [ms]")
fig.savefig("figure_10_bin_mean.png")

#

# fig = plt.figure()
# for filename in filenames:
    # with open(filename) as f:
        # X, Y = zip(*map(literal_eval, f))
        # X = partition_mean(X, 10)
        # Y = convolve(list(partition_mean(Y, 10)), ones(1)/10, "full")
        # plt.scatter(list(X), list(Y), s=1)

# plt.legend(("τ = 0","τ = 5","τ = 10"))
# plt.xlabel("Tick")
# plt.ylabel("Update Execution Runtime [ms]")
# fig.savefig("figure_convolution.png")

# fig = plt.figure()
# for filename in filenames:
    # with open(filename) as f:
        # X, Y = zip(*map(literal_eval, f))
        # X = map(lambda x: x/10, X)
        # Y = map(lambda y: y, Y)
        # plt.scatter(list(X), list(Y), s=1)

# plt.legend(("τ = 0","τ = 5","τ = 10"))
# plt.xlabel("Tick [× 10 ticks]")
# plt.xlim(0,35)
# plt.ylim(0,80)
# plt.ylabel("Query Execution Runtime [ms]")
# fig.savefig("figure_sub.png")

# fig = plt.figure()
# for filename in filenames:
    # with open(filename) as f:
        # X, Y = zip(*map(literal_eval, f))
        # X = list(map(lambda x: x/10, X))
        # Y = list(map(lambda y: y, Y))
        # z = np.polyfit(X[:330], Y[:330], 1)
        # p = np.poly1d(z)
        # plt.scatter(X, Y, s=1)
        # plt.plot(X, p(X))

# plt.legend(("τ = 0","τ = 5","τ = 10"))
# plt.xlabel("Tick [× 10 ticks]")
# plt.xlim(0,35)
# plt.ylim(0,80)
# plt.ylabel("Query Execution Runtime [ms]")
# fig.savefig("figure_sub_fit.png")

