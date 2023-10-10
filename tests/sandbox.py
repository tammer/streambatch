import sys
from pathlib import Path

from time import sleep as time_sleep

sys.path.append(
    str(Path(__file__).parent.parent.joinpath("src"))
)
sys.path.append(
    str(Path(__file__).parent.parent.joinpath("src/streambatch"))
)

from streambatch.module1 import StreambatchConnection

api_key = open('key.txt').read().strip()

connection = StreambatchConnection(api_key=api_key,use_test_api=False)

points = [[3.940705,49.345238]]
query_id1 = connection.request_ndvi(points=points, location_ids=['Somewhere'], sources=['ndvi.streambatch_v2'])

# some_polygon = {
#     'type': 'Polygon',
#     'coordinates': 
#         [[[-94.4545917478666, 41.9792090154671], 
#         [-94.4545448033213, 41.9757220431519], 
#         [-94.4450066084548, 41.9757090969481], 
#         [-94.4450437851949, 41.9792826686391], 
#         [-94.4545917478666, 41.9792090154671]]]
# }
# query_id2 = connection.request_ndvi(polygons=[some_polygon], sources=['ndvi.savgol'])

while not connection.query_done(query_id1):
    print("Waiting for query 1 to finish...")
    time_sleep(10)
print("It finished!")
df = connection.get_data(query_id1)
print(df.tail(5))

# df = connection.get_data(query_id2)
# print(df.tail(5))