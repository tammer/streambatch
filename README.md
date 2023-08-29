Streambatch is an API for time series NDVI data

- **Documentation:** <todo>
- **Website:** https://www.streambatch.io
- **Package source code:** https://github.com/tammer/streambatch


This API provides:

- up-to-date daily time series NDVI from 2013 to today

Features

- 10m resolution
- daily
- global

Installation:

    pip install streambatch

Example:

    from streambatch import StreambatchConnection
    connection = StreambatchConnection(api_key=YOUR_API_KEY)
    query_id = connection.request_ndvi(points=[[long,lat],[long,lat]...],sources=['ndvi.sentinel2'])
    data = connection.get_data(query_id)

