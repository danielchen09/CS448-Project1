import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

FILENAME = "../project1out/tests/_testRandom-BufferMgrBuffMap.txt"
BUFFERMGR = "BufferMgrBuffMap"

class TestOutput:
    def __init__(self, bufferSize, times):
        self.bufferSize = bufferSize.replace("\n", "").replace("BUFFER_SIZE:", "")
        self.times = times.replace("\n", "").split(",")

def graph(outputs, name):
    bufferSizes = []
    time_original = []
    time_new = []
    for output in outputs:
        bufferSizes.append(output.bufferSize)
        time_original.append(output.times[0])
        time_new.append(output.times[1])

    df = pd.DataFrame({
        "x": np.array(bufferSizes).astype(int),
        "original": np.array(time_original).astype(int),
        "new": np.array(time_new).astype(int)
    })
    plt.title(name)
    plt.plot("x", "original", data = df, marker = "", color = "red", linewidth = 1)
    plt.plot("x", "new", data = df, marker = "", color = "blue", linewidth = 1)
    plt.legend()
    plt.xlabel("Buffer Size")
    plt.ylabel("Time(ms)")
    plt.show()


outputs = []
with open(FILENAME, "r") as f:
    lines = f.readlines()

i = 0
while i < len(lines):
    outputs.append(TestOutput(lines[i], lines[i + 1]))
    i += 2

graph(outputs, "test random on " + BUFFERMGR)