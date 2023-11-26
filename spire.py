import tiledb
import numpy as np
import pandas as pd
import uuid

CREATE=False
# Specify the location to save the TileDB array
array_uri = "ais"
# Replace 'data.csv' with the path to your CSV file
file_path = 'large.csv'

repeats=42


def create_fixed_size_uuid_array(size):
    return [str(uuid.uuid4()) for _ in range(size)]

config=tiledb.Config()
config["sm.memory_budget"] =50_000_000
config["sm.memory_budget_var"] =50_000_000
ctx = tiledb.Ctx(config)

if CREATE:
    # Define the array schema for a sparse array with specific dimensions
    # MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,VesselType,Status,Length,Width,Draft,Cargo
    schema = tiledb.ArraySchema(
        domain=tiledb.Domain(
            tiledb.Dim(name="ts_pos_utc", domain=(np.datetime64('2010-01-01T00:00:00.000000'), np.datetime64('2038-01-01T00:00:00.000000')), tile=None, dtype="datetime64[ns]"),
            tiledb.Dim(name="longitude", domain=(-180.0, 180.0), tile=None, dtype="float32"),
            tiledb.Dim(name="latitude", domain=(-90.0, 90.0), tile=None, dtype="float32"),

        ), 
        attrs=[
            tiledb.Attr(name="sog", var=False, nullable=True, dtype="float32"),
            tiledb.Attr(name="cog", var=False, nullable=True, dtype="float32"),
            tiledb.Attr(name="heading", var=False, nullable=True, dtype="float32"),
            tiledb.Attr(name="vesselname", var=True,  nullable=True, dtype="ascii"),
            tiledb.Attr(name="imo", var=True, nullable=True, dtype="ascii"),
            tiledb.Attr(name="callsign", var=True, nullable=True, dtype="ascii"),
            tiledb.Attr(name="vesseltype", var=False, nullable=True, dtype="int16"),
            tiledb.Attr(name="status",  var=False, nullable=True, dtype="int16"),
            tiledb.Attr(name="length",  var=False, nullable=True, dtype="float32"),
            tiledb.Attr(name="width", var=False, nullable=True, dtype="float32"),
            tiledb.Attr(name="draft", var=False, nullable=True, dtype="int16"),
            tiledb.Attr(name="cargo", var=False, nullable=True, dtype="int16"),
            tiledb.Attr(name="mmsi", var=True, nullable=True, dtype="ascii"),
            tiledb.Attr(name="uuid", var=True, nullable=False, dtype="ascii"),
        ],

        sparse=True,
        cell_order='hilbert',
        capacity=100000,
        tile_order=None,
        allows_duplicates=True,
        )

    # # Create the sparse array
    tiledb.SparseArray.create(array_uri, schema)




# # Load the CSV file into a pandas DataFrame
df = pd.read_csv(file_path,  parse_dates=['BaseDateTime'])
df = df.rename(columns=str.lower)

df.mmsi.fillna("",inplace=True)
df.sog.fillna(-1,inplace=True)
df.cog.fillna(-1,inplace=True)
df.heading.fillna(-1,inplace=True)
df.status.fillna(-1,inplace=True)
df.length.fillna(-1,inplace=True)
df.width.fillna(-1,inplace=True)
df.draft.fillna(-1,inplace=True)
df.cargo.fillna(-1,inplace=True)
df.vesselname.fillna("",inplace=True)
df.imo.fillna("",inplace=True)
df.callsign.fillna("",inplace=True)
df.vesseltype.fillna(-1,inplace=True)

# # Open the tiledb array and write the data
with tiledb.open(array_uri, 'w', ctx=ctx) as A:
    for j in range(0, repeats):
        print("This is the", j+1, "insert run.")
        A[df.basedatetime, df.lon, df.lat] = {'sog': df.sog, 'cog': df.cog, 'heading': df.heading, 'vesselname': df.vesselname, 'imo': df.imo, 'callsign': df.callsign, 'vesseltype': df.vesseltype, 'status': df.status, 'length': df.length, 'width': df.width, 'draft': df.draft, 'cargo': df.cargo, 'mmsi': df.mmsi, 'uuid': create_fixed_size_uuid_array(df.shape[0])}
    # A[df.basedatetime, df.lon, df.lat] = {df.sog.to_numpy().ravel(), df.cog.to_numpy().ravel(), df.heading.to_numpy().ravel(), df.vesselname.to_numpy().ravel(), df.imo.to_numpy().ravel(), df.callsign.to_numpy().ravel(), df.vesseltype.to_numpy().ravel(), df.status.to_numpy().ravel(), df.length.to_numpy().ravel(), df.width.to_numpy().ravel(), df.draft.to_numpy().ravel(), df.cargo.to_numpy().ravel(), df.mmsi.to_numpy().ravel()}

# f.query(cond="mmsi == '367003370'").df[:]
