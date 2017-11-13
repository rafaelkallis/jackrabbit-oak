#!/usr/bin/env python3

from matplotlib import use
use("Agg")
from matplotlib.pyplot import savefig, plot, xlabel, ylabel, clf, legend
import numpy as np
from csv import reader
from glob import glob
from math import floor
from argparse import ArgumentParser

parser = ArgumentParser()

parser.add_argument("--tau", nargs="?", default=5)
parser.add_argument("--L", nargs="?", default=30000)

args = parser.parse_args()

filename = max(glob(f"output_*tau{args.tau}_*L{args.L}"))

print(filename)

with open(filename) as f:
    csv_reader = reader(f)
    next(csv_reader)
    ticks, timestamps, query_runtime, trav_index, trav_vol, trav_unprod = zip(*csv_reader)

    ticks = [v/100 for v in map(int, ticks)]
    timestamps = [v/60000 for v in map(int, timestamps)]
    query_runtime = list(map(float, query_runtime))
    trav_index = list(map(float, trav_index))
    trav_vol = list(map(float, trav_vol))
    trav_unprod = list(map(float, trav_unprod))

    xlabel("Time [minutes]")
    ylabel("Query Runtime [ms]")
    plot(timestamps, query_runtime)
    savefig("millis_query_runtime.png")
    clf()

    xlabel("Update Operations [1k]")
    ylabel("Query Runtime [ms]")
    plot(ticks, query_runtime)
    savefig("updates_query_runtime.png")
    clf()

    xlabel("Time [minutes]")
    ylabel("Traversed Index Nodes")
    plot(timestamps, trav_index, label="Total")
    plot(timestamps, trav_vol, label="Volatile")
    plot(timestamps, trav_unprod, label="Unproductive")
    legend()
    savefig("millis_traversed_index_nodes.png")
    clf()

    xlabel("Update Operations [1k]")
    ylabel("Traversed Index Nodes")
    plot(ticks, trav_index, label="Total")
    plot(ticks, trav_vol, label="Volatile")
    plot(ticks, trav_unprod, label="Unproductive")
    legend()
    savefig("updates_traversed_index_nodes.png")
    clf()

    xlabel("Time [minutes]")
    ylabel("Nodes [%]")
    plot(timestamps, [100 * v/t for v,t in zip(trav_vol, trav_index)], label="Volatile")
    plot(timestamps, [100 * v/t for v,t in zip(trav_unprod, trav_index)], label="Unproductive")
    legend()
    savefig("millis_index_nodes_ratio")
    clf()

    xlabel("Update Operations [1k]")
    ylabel("Nodes [%]")
    plot(ticks, [100 * v/t for v,t in zip(trav_vol, trav_index)], label="Volatile")
    plot(ticks, [100 * v/t for v,t in zip(trav_unprod, trav_index)], label="Unproductive")
    legend()
    savefig("updates_index_nodes_ratio")
    clf()

