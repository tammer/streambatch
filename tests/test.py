import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).parent.parent.joinpath("src"))
)

from streambatch.module1 import StreambatchConnection

api_key = "1234567890"
connection = StreambatchConnection(api_key,syncronous=True)

connection.get_ndvi(polygons=[])
connection.get_ndvi(polygons=[],start_date="2020-01-01",end_date="2020-01-31")
connection.get_ndvi(points=[],aggregation="mean")
connection.get_ndvi(points=[],aggregation="mean",start_date="2020-01-01",end_date="2020-01-31")

locations = {
    'loc1': {}
}

