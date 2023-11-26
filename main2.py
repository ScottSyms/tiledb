import tiledb
import numpy as np
import pandas as pd
import uuid

def convert_timestamps_to_epoch_seconds(df):
    for column in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[column]):
            df[column] = df[column].astype('int64') // 10**9
    return df

def create_fixed_size_uuid_array(size):
    return [str(uuid.uuid4()) for _ in range(size)]



# Define the array schema for a sparse array with specific dimensions
# MMSI,BaseDateTime,LAT,LON,SOG,COG,Heading,VesselName,IMO,CallSign,VesselType,Status,Length,Width,Draft,Cargo
schema = tiledb.ArraySchema(
    domain=tiledb.Domain(
        tiledb.Dim(name="mmsi", domain=(None, None), tile=None, dtype="ascii"),
        tiledb.Dim(name="basedatetime", domain=(0, 2147483647), tile=100, dtype="int32"),
        tiledb.Dim(name="lat", domain=(-90.0, 90.0), tile=10, dtype="float32"),
        tiledb.Dim(name="lon", domain=(-180.0, 180.0), tile=10, dtype="float32"),
        tiledb.Dim(name="sog", domain=(-1, 1022), tile=10, dtype="float32"),
        tiledb.Dim(name="cog", domain=(-1, 3600), tile=10, dtype="float32"),
        tiledb.Dim(name="heading", domain=(-1, 511), tile=10, dtype="float32"),
        tiledb.Dim(name="vesselname", domain=(None, None), tile=None, dtype="ascii"),
        tiledb.Dim(name="imo", domain=(None, None), tile=None, dtype="ascii"),
        tiledb.Dim(name="callsign", domain=(None, None), tile=None, dtype="ascii"),
        tiledb.Dim(name="vesseltype", domain=(-1, 1025), tile=10, dtype="int16"),
        tiledb.Dim(name="status", domain=(-1, 256), tile=10, dtype="int16"),
        tiledb.Dim(name="length", domain=(-1, 511.0), tile=10, dtype="float32"),
        tiledb.Dim(name="width", domain=(-1, 511.0), tile=10, dtype="float32"),
        tiledb.Dim(name="draft", domain=(-12, 255), tile=10, dtype="int16"),
        tiledb.Dim(name="cargo", domain=(-1, 255), tile=10, dtype="int16"),

    ),
    sparse=True,
    allows_duplicates=True,
    attrs=[tiledb.Attr(name="vesselid", dtype="ascii")]
)






# Specify the location to save the TileDB array
array_uri = "sparse"

# Create the sparse array
tiledb.SparseArray.create(array_uri, schema)


# Replace 'data.csv' with the path to your CSV file
file_path = 'large.csv'

# Load the CSV file into a pandas DataFrame
df = pd.read_csv(file_path,  parse_dates=['BaseDateTime'])
df = df.rename(columns=str.lower)

# replace all "nan" with empty values
# df=df.fillna("")

df = convert_timestamps_to_epoch_seconds(df)

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

column_arrays = {column: df[column].values for column in df.columns}

# Isolate the column headings from the loaded CSV file
arraykeys=list(column_arrays.keys())
print(arraykeys[0])

# Sample a dictionary item and count the values
sizeofarray=len(column_arrays[arraykeys[0]])

# Create a list of UUIDs to populate in the TileDB cells.
data=create_fixed_size_uuid_array(sizeofarray)
# print("Array keys: ", arraykeys, sizeofarray, data)


dimensions=[]
for i in arraykeys:
    dimensions.append(np.array(column_arrays[i]))

print("Dimensions: ", dimensions)

print("number of keys", sizeofarray, len(arraykeys), len(dimensions))

# Open the tiledb array and write the data
with tiledb.open(array_uri, 'w') as A:
    A[dimensions[0], dimensions[1], dimensions[2], dimensions[3], dimensions[4], dimensions[5], dimensions[6], dimensions[7], dimensions[8], dimensions[9], dimensions[10], dimensions[11], dimensions[12], dimensions[13], dimensions[14], dimensions[15] ] = data



