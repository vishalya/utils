import sys
import pyarrow as pa
import pyarrow.parquet as pq
import glob

files = glob.glob("/tmp/data/*.parquet")
merged =  pq.read_table(files[0])
i = 1
for f in files[1:]:
    i = i + 1
    try:
        table = pq.read_table(f)
        merged = pa.concat_tables([merged, table])
    except:
        print(f'appending failed at {i} {f}')
        e = sys.exc_info()[0]
        print("Got error .... ")
        print(f'{e}')

pq.write_table(merged, "/tmp/merged.parquet")
