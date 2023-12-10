import tiledb
import numpy as np
import pandas as pd
import os
from pyarrow import csv
import time


os.environ["AWS_ACCESS_KEY_ID"] = "vh8wYUYFt1mQIaENBB3Y"
os.environ["AWS_SECRET_ACCESS_KEY"] = "Ow8RNtDA3TGfm3Hhg3icPGpIl6NNgMHBvJrDFChr"


# Specify the location to save the TileDB array

# Array for local minio instance; URL and port is stored in the configuration context.
array_uri = "s3://ais/marinecadastre"

# Configuration parameters for tiledb context
config = tiledb.Config()
config["vfs.s3.scheme"] = "http"
config["vfs.s3.region"] = ""
config["vfs.s3.endpoint_override"] = "localhost:9000"
config["vfs.s3.use_virtual_addressing"] = "false"
ctx = tiledb.Ctx(config)

f = tiledb.open(array_uri, "r", ctx=ctx)
print(f.schema)

print("Looking for MMSI")
start = time.time()
t = f.query(
    dims=["report_timestamp", "latitude", "longitude"],
    attrs=["mmsi", "uuid"],
    cond="mmsi == '367003370'",
    return_arrow=False,
).df[:]
print(len(t), time.time() - start)


print("\nGetting a date span.")
d1 = pd.to_datetime("2017-12-12T00:00:00").to_numpy().astype(np.int64)
d2 = pd.to_datetime("2017-12-12T00:30:00").to_numpy().astype(np.int64)
print(f"\n results between {d1} and {d2}.")

start = time.time()

t = f.query(
    dims=["report_timestamp", "latitude", "longitude"],
    attrs=["uuid"],
    cond=f"report_timestamp > {d1} and report_timestamp < {d2} ",
    return_arrow=False,

)
end = time.time() - start
print(f"Completed query in {end:.2f}")
t = t.df[:]
print("Pulled ", len(t), "results in ", time.time() - start, "seconds.")
print(t[:5])

print("\nGetting one report.")
start = time.time()

t = f.query(
    dims=["report_timestamp", "latitude", "longitude"],
    attrs=["mmsi", "uuid"],
    cond="uuid == '95e7feba-e43d-40d7-b48c-5b878dcc310d'",
    return_arrow=False,

).df[:]
print(len(t), time.time() - start)
print(t[:5])

# print("\nhow many.")
# start=time.time()

# t=f.query(dims=['latitude']).df[:]
# print(len(t), time.time() - start)
