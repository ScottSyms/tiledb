import tiledb
import numpy as np

# Define the array schema for a sparse array with specific dimensions
schema = tiledb.ArraySchema(
    domain=tiledb.Domain(
        tiledb.Dim(name="mmsi", domain=(None, None), tile=None, dtype="ascii"),
        tiledb.Dim(name="basedatetime", domain=(0, 2147483647), tile=100, dtype="int32"),
        tiledb.Dim(name="lat", domain=(-90.0, 90.0), tile=10, dtype="float32"),
        tiledb.Dim(name="lon", domain=(-180.0, 180.0), tile=10, dtype="float32")
    ),
    sparse=True,
    attrs=[tiledb.Attr(name="data", dtype="float64")]
)

# Specify the location to save the TileDB array
array_uri = "sparse"

# Create the sparse array
tiledb.SparseArray.create(array_uri, schema)

