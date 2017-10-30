#!/usr/bin/env python3

from glob import glob
from ast import literal_eval
import matplotlib.pyplot as plt
import numpy as np

filenames = sorted(list(glob("output_query_time*")))

fig = plt.figure()
for filename in filenames:
    with open(filename) as f:
        X, Y = zip(*map(literal_eval, f))
        X = map(lambda x: x/10, X)
        Y = map(lambda y: y/1000, Y)
        plt.scatter(list(X), list(Y), s=1)

plt.legend(("τ = 0","τ = 5","τ = 10"))
plt.xlabel("Tick [× 10 ticks]")
plt.ylabel("Query Execution Runtime [s]")
fig.savefig("figure_all.png")

fig = plt.figure()
for filename in filenames:
    with open(filename) as f:
        X, Y = zip(*map(literal_eval, f))
        X = map(lambda x: x/10, X)
        Y = map(lambda y: y, Y)
        plt.scatter(list(X), list(Y), s=1)

plt.legend(("τ = 0","τ = 5","τ = 10"))
plt.xlabel("Tick [× 10 ticks]")
plt.xlim(0,35)
plt.ylim(0,80)
plt.ylabel("Query Execution Runtime [ms]")
fig.savefig("figure_sub.png")

fig = plt.figure()
for filename in filenames:
    with open(filename) as f:
        X, Y = zip(*map(literal_eval, f))
        X = list(map(lambda x: x/10, X))
        Y = list(map(lambda y: y, Y))
        z = np.polyfit(X[:330], Y[:330], 1)
        p = np.poly1d(z)
        plt.scatter(X, Y, s=1)
        plt.plot(X, p(X))

plt.legend(("τ = 0","τ = 5","τ = 10"))
plt.xlabel("Tick [× 10 ticks]")
plt.xlim(0,35)
plt.ylim(0,80)
plt.ylabel("Query Execution Runtime [ms]")
fig.savefig("figure_sub_fit.png")

