#!/usr/bin/env python3

from matplotlib import use
use("Agg")
from matplotlib.pyplot import savefig, plot, xlabel, ylabel, clf, legend, axvline
import numpy as np
from csv import reader
from glob import glob
from math import floor

def tick_milestones(ticks, timestamps, interval):
    last_milestone = -1
    for tick, timestamp in zip(ticks, timestamps):
        current_milestone = floor(timestamp/interval)
        if current_milestone > last_milestone:
            yield tick
        last_milestone = current_milestone

filename = max(glob("query_output_*_synthetic_tau5_L30000"))

with open(filename) as f:
    csv_reader = reader(f)
    next(csv_reader)
    ticks, timestamps, query_runtime, trav_index, trav_vol, trav_unprod = zip(*csv_reader)

    ticks = np.array(list(map(int, ticks)))
    timestamps = np.array(list(map(int, timestamps)))
    query_runtime = np.array(list(map(float, query_runtime)))
    trav_index = np.array(list(map(float, trav_index)))
    trav_vol = np.array(list(map(float, trav_vol)))
    trav_unprod = np.array(list(map(float, trav_unprod)))

    milestones = np.array(list(tick_milestones(ticks, timestamps, 60000)))

    xlabel("Update Operations [1k]")
    ylabel("Avg. Query Runtime [ms]")
    plot(ticks/100, query_runtime)
    for m in milestones:
        axvline(x=m)
    savefig("query_runtime_synthetic.png")
    clf()

    xlabel("Update Operations [1k]")
    ylabel("Traversed Nodes per Query [1k]")
    plot(ticks/100, trav_index/1000, "C7", label="Total")
    plot(ticks/100, trav_vol/1000, "C0", label="Volatile")
    plot(ticks/100, trav_unprod/1000, "C1", label="Unproductive")
    legend()
    for m in milestones:
        axvline(x=m)
    savefig("trav_nodes_synthetic.png")
    clf()

    xlabel("Update Operations [1k]")
    ylabel("Node Ratio")
    plot(ticks/100, trav_vol/trav_index, "C0", label="Volatile")
    plot(ticks/100, trav_unprod/trav_index, "C1", label="Unproductive")
    legend()
    for m in milestones:
        axvline(x=m)
    savefig("trav_node_ratio_synthetic.png")
    clf()

