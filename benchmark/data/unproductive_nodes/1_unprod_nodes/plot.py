#!/usr/bin/env python3

from matplotlib import use
use("Agg")
from matplotlib.pyplot import savefig, plot, xlabel, ylabel, clf, legend
import numpy as np
from csv import reader
from glob import glob
from math import floor

filename = max(glob("output_*"))

print(filename)

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

    xlabel("Time [minutes]")
    ylabel("Query Runtime [ms]")
    plot(timestamps/60000, query_runtime)
    savefig("millis_query_runtime.png")
    clf()

    xlabel("Update Operations [1k]")
    ylabel("Query Runtime [ms]")
    plot(ticks/100, query_runtime)
    savefig("updates_query_runtime.png")
    clf()

    xlabel("Time [minutes]")
    ylabel("Traversed Nodes per Query [1k]")
    plot(timestamps/60000, trav_index/1000, "C7", label="Total")
    plot(timestamps/60000, trav_vol/1000, "C0", label="Volatile")
    plot(timestamps/60000, trav_unprod/1000, "C1", label="Unproductive")
    legend()
    savefig("millis_traversed_index_nodes.png")
    clf()

    xlabel("Update Operations [1k]")
    ylabel("Traversed Nodes per Query [1k]")
    plot(ticks/100, trav_index/1000, "C7", label="Total")
    plot(ticks/100, trav_vol/1000, "C0", label="Volatile")
    plot(ticks/100, trav_unprod/1000, "C1", label="Unproductive")
    legend()
    savefig("updates_traversed_index_nodes.png")
    clf()

    xlabel("Time [minutes]")
    ylabel("Node Density")
    plot(timestamps/60000, trav_vol/trav_index, "C0", label="Volatile")
    plot(timestamps/60000, trav_unprod/trav_index, "C1", label="Unproductive")
    legend()
    savefig("millis_index_node_density")
    clf()

    xlabel("Update Operations [1k]")
    ylabel("Node Density")
    plot(ticks/100, trav_vol/trav_index, "C0", label="Volatile")
    plot(ticks/100, trav_unprod/trav_index, "C1", label="Unproductive")
    legend()
    savefig("updates_index_node_density")
    clf()

