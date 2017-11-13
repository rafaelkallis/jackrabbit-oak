#!/usr/bin/env python3

from matplotlib import use
use("Agg")
from matplotlib.pyplot import savefig, plot, xlabel, ylabel, clf, legend
import numpy as np
from csv import reader
from glob import glob
from math import floor
from re import search
from statistics import mean, median

filenames = glob("output_*")
data = dict()

for filename in filenames:
    L = int(search("_L(?P<L>[0-9]+)", filename).group("L"))
    with open(filename) as f:
        csv_reader = reader(f)
        next(csv_reader)
        ticks, timestamps, query_runtime, trav_index, trav_vol, trav_unprod = zip(*csv_reader)
        data[L] = {
            "ticks": np.array(list(map(int, ticks))),
            "timestamps": np.array(list(map(int, timestamps))),
            "query_runtime": np.array(list(map(float, query_runtime))),
            "trav_index": np.array(list(map(float, trav_index))),
            "trav_vol": np.array(list(map(float, trav_vol))),
            "trav_unprod": np.array(list(map(float, trav_unprod)))
        }

xlabel("Update Operations [1k]")
ylabel("Query Runtime [ms]")
plot(data[15000]["ticks"]/100, data[15000]["query_runtime"], label="L = 15 s")
plot(data[30000]["ticks"]/100, data[30000]["query_runtime"], label="L = 30 s")
plot(data[45000]["ticks"]/100, data[45000]["query_runtime"], label="L = 45 s")
legend()
savefig("query_runtime_Ls.png")
clf()

xlabel("Update Operations [1k]")
ylabel("Traversed Unproductive Nodes [1k]")
plot(data[15000]["ticks"]/100, data[15000]["trav_unprod"]/1000, label="L = 15 s")
plot(data[30000]["ticks"]/100, data[30000]["trav_unprod"]/1000, label="L = 30 s")
plot(data[45000]["ticks"]/100, data[45000]["trav_unprod"]/1000, label="L = 45 s")
legend()
savefig("traversed_unprod_nodes_Ls.png")
clf()

xlabel("Update Operations [1k]")
ylabel("Unproductive Node Density")
plot(data[15000]["ticks"]/100, data[15000]["trav_unprod"]/data[15000]["trav_index"], label="L = 15 s")
plot(data[30000]["ticks"]/100, data[30000]["trav_unprod"]/data[30000]["trav_index"], label="L = 30 s")
plot(data[45000]["ticks"]/100, data[45000]["trav_unprod"]/data[45000]["trav_index"], label="L = 45 s")
legend()
savefig("unprod_node_density_Ls.png")
clf()

Ls = np.array(sorted(list(data.keys())))

xlabel("Sliding Window Length (L) [seconds]")
ylabel("Query Runtime [ms]")
plot(Ls/1000, [data[L]["query_runtime"][999] for L in Ls],"o")
savefig("L_query_runtime.png")
clf()

xlabel("Sliding Window Length (L) [seconds]")
ylabel("Traversed Unproductive Nodes [100]")
plot(Ls/1000, [data[L]["trav_unprod"][999]/100 for L in Ls], "o")
savefig("L_unprod_nodes.png")
clf()

xlabel("Sliding Window Length (L) [seconds]")
ylabel("Unproductive Node Density")
plot(Ls/1000, [mean((data[L]["trav_unprod"][995:1004])/(data[L]["trav_index"][995:1004])) for L in Ls], "o") 
savefig("L_unprod_node_density.png")
clf()
