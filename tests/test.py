import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).parent.parent.joinpath("src"))
)

from streambatch.module1 import StreambatchConnection

api_key = open('key.txt').read().strip()
connection = StreambatchConnection(api_key,debug=True)
points = [[3.940705,49.345238]]

# data = connection.get_ndvi(points=points,sources=["ndvi.sentinel2"])
query_id = connection.request_ndvi(points=points,sources=["ndvi.sentinel2",'ndvi.landast'])
data = connection.get_data(query_id)
print(data)