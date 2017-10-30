#!/usr/bin/env python3

from glob import glob
from ast import literal_eval
import matplotlib.pyplot as plt

filenames = sorted(list(glob("output_write_time*")))

fig = plt.figure()
for filename in filenames:
    with open(filename) as f:
        X, Y = zip(*map(literal_eval, f))
        X = map(lambda x: x/60000, X)
        plt.scatter(list(X), list(Y), s=1)

plt.legend(("τ = 0","τ = 5","τ = 10"))
plt.xlabel("Time [minutes]")
plt.ylabel("Update Execution Runtime [ms]")
plt.ylim(0,60)
fig.savefig("figure.png")


# fig = plt.figure()
# for filename in filenames:
   # with open(filename) as f:
       # X, Y = zip(*map(literal_eval, f))
       # X = map(lambda x: x/1000, X)
       # Y = map(lambda y: y, Y)
       # plt.scatter(list(X), list(Y), s=1)

#plt.legend(("τ = 0","τ = 5","τ = 10"))
#plt.xlabel("Time [seconds]")
#plt.xlim(0,60)
#plt.ylim(0,125)
#plt.ylabel("Query Execution Runtime [ms]")
#fig.savefig("figure_sub.png")

