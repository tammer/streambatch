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

connection = StreambatchConnection(open('key.txt').read().strip(),debug=True)
points = [[3.940705,49.345238],[3.940705,49.345238]]
# query_id = connection.request_ndvi(points=points, sources=['ndvi.sentinel2','ndvi.landsat'])
query_id = connection.request_ndvi(points=points, sources=['ndvi.savgol'])
df = connection.get_data(query_id)
print(df.tail(20))