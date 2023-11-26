import tiledb
import numpy as np
import pandas as pd
import uuid

CREATE=False
# Specify the location to save the TileDB array
array_uri = "sparse"

repeats=10

def convert_timestamps_to_epoch_seconds(df):
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = df[column].astype('int64') // 10**9
    return df

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
            tiledb.Attr(name="sog", nullable=True, dtype="float32"),
            tiledb.Attr(name="cog", nullable=True, dtype="float32"),
            tiledb.Attr(name="heading", nullable=True, dtype="float32"),
            tiledb.Attr(name="vesselname",   dtype="ascii"),
            tiledb.Attr(name="imo", nullable=True, dtype="ascii"),
            tiledb.Attr(name="callsign", nullable=True, dtype="ascii"),
            tiledb.Attr(name="vesseltype", nullable=True, dtype="int16"),
            tiledb.Attr(name="status",  dtype="int16"),
            tiledb.Attr(name="length",  dtype="float32"),
            tiledb.Attr(name="width", nullable=True, dtype="float32"),
            tiledb.Attr(name="draft", nullable=True, dtype="int16"),
            tiledb.Attr(name="cargo", nullable=True, dtype="int16"),
            tiledb.Attr(name="mmsi", nullable=True, dtype="ascii"),
        ],

        sparse=True,
        cell_order='hilbert',
        capacity=100000,
        tile_order=None,
        allows_duplicates=True,
        )




    # # Create the sparse array
    tiledb.SparseArray.create(array_uri, schema)


# # Replace 'data.csv' with the path to your CSV file
file_path = 'large.csv'

# # Load the CSV file into a pandas DataFrame
df = pd.read_csv(file_path,  parse_dates=['BaseDateTime'])
df = df.rename(columns=str.lower)

# attributes=[]
# for index, row in df.iterrows():
#     attributes.append([row["sog"],  row["cog"], row["heading"], row["vesselname"], row["imo"], row["callsign"], row["vesseltype"], row["status"], row["length"], row["width"], row["draft"], row["cargo"], row["mmsi"] ])

# print(attributes)

# # replace all "nan" with empty values
# # df=df.fillna("")

# df = convert_timestamps_to_epoch_seconds(df)

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
    for j in range(1, repeats):
        print("This is the", j, "insert run.")
        A[df.basedatetime, df.lon, df.lat] = {'sog': df.sog, 'cog': df.cog, 'heading': df.heading, 'vesselname': df.vesselname, 'imo': df.imo, 'callsign': df.callsign, 'vesseltype': df.vesseltype, 'status': df.status, 'length': df.length, 'width': df.width, 'draft': df.draft, 'cargo': df.cargo, 'mmsi': df.mmsi}
    # A[df.basedatetime, df.lon, df.lat] = {df.sog.to_numpy().ravel(), df.cog.to_numpy().ravel(), df.heading.to_numpy().ravel(), df.vesselname.to_numpy().ravel(), df.imo.to_numpy().ravel(), df.callsign.to_numpy().ravel(), df.vesseltype.to_numpy().ravel(), df.status.to_numpy().ravel(), df.length.to_numpy().ravel(), df.width.to_numpy().ravel(), df.draft.to_numpy().ravel(), df.cargo.to_numpy().ravel(), df.mmsi.to_numpy().ravel()}

# f.query(cond="mmsi == '367003370'").df[:]
