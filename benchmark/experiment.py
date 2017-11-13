#!/usr/bin/env python3

from subprocess import run

def compile():
    run(["mvn","clean","package"])

def execute(tau, L):
    args = ["java","-jar"]
    if tau != None:
        args.append(f"-DvolatilityThreshold={tau}")
    if L != None:
        args.append(f"-DslidingWindowLength={L}")
    args.append("target/benchmark-1.0.0-jar-with-dependencies.jar")
    run(args)

taus = [0,1,2,3,4,5,6,7,8,9,10,20,30,50]
Ls = [0,30000,60000,90000,120000,150000,180000,210000,240000,270000,30000]

# compile()

for tau in taus:
    execute(tau, None)

for L in Ls:
    execute(None, L)



