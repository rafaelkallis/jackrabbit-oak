#!/usr/bin/env python3

from glob import glob
from ast import literal_eval
import matplotlib.pyplot as plt

filenames = sorted(list(glob("output_volatile_nodes*")))

fig = plt.figure()

for filename in filenames:
    with open(filename) as f:
        X, Y = zip(*map(literal_eval, f))
        X = map(lambda x: x/60000, X)
        # Y = map(lambda y: y/1000, Y)
        plt.scatter(list(X), list(Y), s=1)

plt.legend(("τ = 0","τ = 5","τ = 10"))
plt.xlabel("Time [minutes]")
plt.ylabel("# Volatile Index Nodes")
fig.savefig("figure_all.png")

fig = plt.figure()

for filename in filenames:
    with open(filename) as f:
        X, Y = zip(*map(literal_eval, f))
        X = map(lambda x: x/60000, X)
        # Y = map(lambda y: y/1000, Y)
        plt.scatter(list(X), list(Y), s=1)

plt.ylim(0, 400)
plt.legend(("τ = 0","τ = 5","τ = 10"))
plt.xlabel("Time [minutes]")
plt.ylabel("# Volatile Index Nodes")
fig.savefig("figure_sub.png")

