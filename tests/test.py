import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).parent.parent.joinpath("src"))
)
sys.path.append(
    str(Path(__file__).parent.parent.joinpath("src/streambatch"))
)

from streambatch.module1 import StreambatchConnection

api_key = open('key.txt').read().strip()

connection = StreambatchConnection(api_key=api_key)
points = [[993.940705,9949.345238]]
query_id = connection.request_ndvi(points=points, sources=['ndvi.sentinel2'])
df = connection.get_data(query_id)
print(df.tail(5))