import tiledb
import numpy as np
import pandas as pd
import uuid
import random
import csv
import time
# import os
# import sys
from pyarrow import csv


# os.environ["AWS_ACCESS_KEY_ID"] = "vh8wYUYFt1mQIaENBB3Y"
# os.environ["AWS_SECRET_ACCESS_KEY"] = "Ow8RNtDA3TGfm3Hhg3icPGpIl6NNgMHBvJrDFChr"


# Create the database
CREATE = True

# How long to wait after S3 operations
AWS_WAIT=10

# Specify the location to save the TileDB array

# Local file storage
# array_uri="ais"

# Array for local minio instance; URL and port is stored in the configuration context.
array_uri = "s3://ais/marinecadastre"

# csv to process.
# This file should be in the proper projection
file_path = "small.csv"

# How many times to repeat the load of data.  Useful for testing.
repeats = 1

# array of sources
source_array = ["source1", "source2", "source3", "source4", "source5", "source6"]

# Create a fixed size array of random selections from a pool of strings
def create_source_array(size):
    global source_array
    return [random.choice(source_array) for _ in range(size)]

# Create a fixed size array of UUIDs.  These can be used to track individual position reports.
def create_uuid_array(size):
    return [str(uuid.uuid4()) for _ in range(size)]


# Configuration parameters for Minio tiledb context
minio_cfg = tiledb.Config()
minio_cfg["sm.check_coord_oob"] = False  # don't check to see if values are in the dimensional range
minio_cfg["sm.memory_budget"] = 50_000_000
minio_cfg["sm.memory_budget_var"] = 50_000_000
minio_cfg["sm.tile_cache_size"] = 50_000_000
minio_cfg["py.init_buffer_bytes"] = 1024**2 * 50 * 14

# Settings for storing the data in a local installation of Minio
minio_cfg["vfs.s3.scheme"] = "http"
minio_cfg["vfs.s3.region"] = ""
minio_cfg["vfs.s3.endpoint_override"] = "localhost:9000"
minio_cfg["vfs.s3.use_virtual_addressing"] = "false"
minio_cfg["vfs.s3.aws_access_key_id"] = "vh8wYUYFt1mQIaENBB3Y"
minio_cfg["vfs.s3.aws_secret_access_key"] = "Ow8RNtDA3TGfm3Hhg3icPGpIl6NNgMHBvJrDFChr"


# Wrap the configuration into a context
minio_ctx = tiledb.Ctx(minio_cfg)

# Create a virtual filesystem object
vfs = tiledb.VFS(ctx=minio_ctx)

if CREATE:
    # Is the bucket empty and created?
    if vfs.is_bucket("s3://ais"):
        print("Deleting existing bucket")
        vfs.remove_bucket("s3://ais")
        time.sleep(AWS_WAIT)

    print("Creating  bucket")
    vfs.create_bucket("s3://ais")
    time.sleep(AWS_WAIT)

    # Create a filter list
    filter_list = tiledb.FilterList(
        [tiledb.BitShuffleFilter(), tiledb.ZstdFilter(level=5)]
    )

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
            tiledb.Attr(
                name="sog",
                var=False,
                nullable=True,
                dtype="float32",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="cog",
                var=False,
                nullable=True,
                dtype="float32",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="heading",
                var=False,
                nullable=True,
                dtype="float32",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="vesselname",
                var=True,
                nullable=True,
                dtype="ascii",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="imo", var=True, nullable=True, dtype="ascii", filters=filter_list
            ),
            tiledb.Attr(
                name="callsign",
                var=True,
                nullable=True,
                dtype="ascii",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="vesseltype",
                var=False,
                nullable=True,
                dtype="int16",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="status",
                var=False,
                nullable=True,
                dtype="int16",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="length",
                var=False,
                nullable=True,
                dtype="float32",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="width",
                var=False,
                nullable=True,
                dtype="float32",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="draft",
                var=False,
                nullable=True,
                dtype="int16",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="cargo",
                var=False,
                nullable=True,
                dtype="int16",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="mmsi", var=True, nullable=True, dtype="ascii", filters=filter_list
            ),
            tiledb.Attr(
                name="uuid",
                var=True,
                nullable=False,
                dtype="ascii",
                filters=filter_list,
            ),
            tiledb.Attr(
                name="source",
                var=True,
                nullable=False,
                dtype="ascii",
                filters=filter_list,
            ),
        ],
        sparse=True,
        cell_order="hilbert",
        capacity=1_000_000,
        tile_order=None,
        allows_duplicates=True,
    )

    # # Create the sparse array
    tiledb.SparseArray.create(array_uri, schema, ctx=minio_ctx)

# Read the CSV file
df = csv.read_csv("large.csv").to_pandas()

# convert column headings to lower case
df = df.rename(columns=str.lower)

# Open the tiledb array and write the data arrays
with tiledb.open(array_uri, "w", ctx=minio_ctx) as A:
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
            "uuid": create_uuid_array(df.shape[0]),
            "source": create_source_array(df.shape[0]),
        }
