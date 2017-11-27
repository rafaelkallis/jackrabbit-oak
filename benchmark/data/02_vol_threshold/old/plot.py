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
    tau = int(search("_tau(?P<tau>.*)_", filename).group("tau"))
    with open(filename) as f:
        csv_reader = reader(f)
        next(csv_reader)
        ticks, timestamps, query_runtime, trav_index, trav_vol, trav_unprod = zip(*csv_reader)
        data[tau] = {
            "ticks": np.array(list(map(int, ticks))),
            "timestamps": np.array(list(map(int, timestamps))),
            "query_runtime": np.array(list(map(float, query_runtime))),
            "trav_index": np.array(list(map(float, trav_index))),
            "trav_vol": np.array(list(map(float, trav_vol))),
            "trav_unprod": np.array(list(map(float, trav_unprod)))
        }

xlabel("Update Operations [1k]")
ylabel("Query Runtime [ms]")
plot(data[1]["ticks"]/100, data[1]["query_runtime"], label="τ = 1")
plot(data[5]["ticks"]/100, data[5]["query_runtime"], label="τ = 5")
plot(data[10]["ticks"]/100, data[10]["query_runtime"], label="τ = 10")
legend()
savefig("query_runtime.png")
clf()

xlabel("Update Operations [1k]")
ylabel("Traversed Unproductive Nodes [1k]")
plot(data[1]["ticks"]/100, data[1]["trav_unprod"]/1000, label="τ = 1")
plot(data[5]["ticks"]/100, data[5]["trav_unprod"]/1000, label="τ = 5")
plot(data[10]["ticks"]/100, data[10]["trav_unprod"]/1000, label="τ = 10")
legend()
savefig("traversed_unprod_nodes.png")
clf()

xlabel("Update Operations [1k]")
ylabel("Unproductive Node Density")
plot(data[1]["ticks"]/100, data[1]["trav_unprod"]/data[1]["trav_index"], label="τ = 1")
plot(data[5]["ticks"]/100, data[5]["trav_unprod"]/data[5]["trav_index"], label="τ = 5")
plot(data[10]["ticks"]/100, data[10]["trav_unprod"]/data[10]["trav_index"], label="τ = 10")
legend()
savefig("unprod_node_density")
clf()

taus = sorted(list(data.keys()))

xlabel("Volatility Threshold (τ)")
ylabel("Query Runtime [ms]")
plot(taus, [data[tau]["query_runtime"][999] for tau in taus],"o")
savefig("tau_query_runtime.png")
clf()

xlabel("Volatility Threshold (τ)")
ylabel("Traversed Unproductive Nodes [1k]")
plot(taus, [data[tau]["trav_unprod"][999]/1000 for tau in taus], "o")
savefig("tau_unprod_nodes.png")
clf()

xlabel("Volatility Threshold (τ)")
ylabel("Unproductive Node Density")
plot(taus, [mean((data[tau]["trav_unprod"][995:1004])/(data[tau]["trav_index"][995:1004])) for tau in taus], "o") 
savefig("tau_unprod_node_density.png")
clf()
