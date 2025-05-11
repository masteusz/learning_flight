import pyarrow as pa
import pyarrow.flight

client = pa.flight.connect("grpc://0.0.0.0:8815")