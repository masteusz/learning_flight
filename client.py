import pyarrow as pa
import pyarrow.flight as flight

client = flight.connect("grpc://0.0.0.0:8815")
data_table = pa.table(
    [["Mario", "Luigi", "Peach"]],
    names=["Character"]
)
upload_descriptor = flight.FlightDescriptor.for_path("uploaded.parquet")
writer, _ = client.do_put(upload_descriptor, data_table.schema)
writer.write_table(data_table)
writer.close()

_fl = client.get_flight_info(upload_descriptor)
descriptor = _fl.descriptor
print("Path:", descriptor.path[0].decode('utf-8'), "Rows:", _fl.total_records, "Size:", _fl.total_bytes)
print("=== Schema ===")
print(_fl.schema)
print("==============")

reader = client.do_get(_fl.endpoints[0].ticket)
read_table = reader.read_all()
print(read_table.to_pandas().head())

client.do_action(flight.Action("drop_dataset", "uploaded.parquet".encode('utf-8')))

for fl in client.list_flights():
    descriptor = fl.descriptor
    print("Path:", descriptor.path[0].decode('utf-8'), "Rows:", fl.total_records, "Size:", fl.total_bytes)
    print("=== Schema ===")
    print(fl.schema)
    print("==============")
    print("")