import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).parent.parent.joinpath("src"))
)

from streambatch.module1 import StreambatchConnection

api_key = open('key.txt').read().strip()
connection = StreambatchConnection(api_key,syncronous=True,debug=True)
points = [[3.940705,49.345238]]

data = connection.get_ndvi(points=points,sources=["ndvi.sentinel2"])
print(data)