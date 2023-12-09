import tiledb
import numpy as np
import pandas as pd
import uuid
import csv
import sys
from pyarrow import csv

# Create the database
CREATE = False

# Specify the location to save the TileDB array
array_uri = "ais"

# csv to process.
# This file should be in the proper projection
file_path = "large.csv"

# How many times to repeat the load of data.  Useful for testing.
repeats = 41

# Create a fixed size array of UUIDs.  These can be used to track individual position reports.
def create_fixed_size_uuid_array(size):
    return [str(uuid.uuid4()) for _ in range(size)]

# Configuration parameters for tiledb context   
config = tiledb.Config()
config["sm.memory_budget"] = 50_000_000
config["sm.memory_budget_var"] = 50_000_000
config["sm.tile_cache_size"] = 50_000_000
config["py.init_buffer_bytes"] = 1024**2 * 50 * 14
ctx = tiledb.Ctx(config)

if CREATE:
    # Create a filter list
    filter_list = tiledb.FilterList([tiledb.ZstdFilter(level=5)])

    # Define the array schema for a sparse array with specific dimensions
    # MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,VesselType,Status,Length,Width,Draft,Cargo
    schema = tiledb.ArraySchema(
        domain=tiledb.Domain(
            # Define the dimensions of time, longitude and latitude.
            # The timestamps extend from 2010 to 2038
            tiledb.Dim(
                name="report_timestamp",
                domain=(
                    np.datetime64("2010-01-01T00:00:00.000000"),
                    np.datetime64("2038-01-01T00:00:00.000000"),
                ),
                tile=None,
                dtype="datetime64[ns]",
            ),
            tiledb.Dim(
                name="longitude", domain=(-180.0, 180.0), tile=None, dtype="float32"
            ),
            tiledb.Dim(
                name="latitude", domain=(-90.0, 90.0), tile=None, dtype="float32"
            ),
        ),
        attrs=[
            tiledb.Attr(name="sog", var=False, nullable=True, dtype="float32", filters=filter_list),
            tiledb.Attr(name="cog", var=False, nullable=True, dtype="float32", filters=filter_list),
            tiledb.Attr(name="heading", var=False, nullable=True, dtype="float32", filters=filter_list),
            tiledb.Attr(name="vesselname", var=True, nullable=True, dtype="ascii", filters=filter_list),
            tiledb.Attr(name="imo", var=True, nullable=True, dtype="ascii", filters=filter_list),
            tiledb.Attr(name="callsign", var=True, nullable=True, dtype="ascii", filters=filter_list),
            tiledb.Attr(name="vesseltype", var=False, nullable=True, dtype="int16", filters=filter_list),
            tiledb.Attr(name="status", var=False, nullable=True, dtype="int16", filters=filter_list),
            tiledb.Attr(name="length", var=False, nullable=True, dtype="float32", filters=filter_list),
            tiledb.Attr(name="width", var=False, nullable=True, dtype="float32", filters=filter_list),
            tiledb.Attr(name="draft", var=False, nullable=True, dtype="int16", filters=filter_list),
            tiledb.Attr(name="cargo", var=False, nullable=True, dtype="int16", filters=filter_list),
            tiledb.Attr(name="mmsi", var=True, nullable=True, dtype="ascii", filters=filter_list),
            tiledb.Attr(name="uuid", var=True, nullable=False, dtype="ascii", filters=filter_list),
        ],
        sparse=True,
        cell_order="hilbert",
        capacity=1_000_000,
        tile_order=None,
        allows_duplicates=True,
    )

    # # Create the sparse array
    tiledb.SparseArray.create(array_uri, schema)

# Read the CSV file
df=csv.read_csv('large.csv').to_pandas()

# convert column headings to lower case
df = df.rename(columns=str.lower)

# Open the tiledb array and write the data arrays
with tiledb.open(array_uri, "w", ctx=ctx) as A:
    for j in range(0, repeats):
        print("This is the", j + 1, "insert run.")
        A[df.basedatetime, df.lon, df.lat] = {
            "sog": df.sog,
            "cog": df.cog,
            "heading": df.heading,
            "vesselname": df.vesselname,
            "imo": df.imo,
            "callsign": df.callsign,
            "vesseltype": df.vesseltype,
            "status": df.status,
            "length": df.length,
            "width": df.width,
            "draft": df.draft,
            "cargo": df.cargo,
            "mmsi": df.mmsi,
            "uuid": create_fixed_size_uuid_array(df.shape[0]),
        }

