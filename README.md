<img src="http://www.tammer.com/logo.png" width="300">

Streambatch is an API for time series NDVI data

- **Documentation:** https://docs.streambatch.io/reference/streambatch-api
- **Website:** https://www.streambatch.io
- **Package source code:** https://github.com/tammer/streambatch

This API provides up-to-date daily time series NDVI from 2013 to today for any location on Earth. You send a request to the server specifying any number of locations (single points or polygons).  The request is processed asynchronously. Data is returned as a pandas dataframe.

### Installation

    pip install streambatch

### Example

    from streambatch import StreambatchConnection
    connection = StreambatchConnection(api_key=YOUR_API_KEY)
    query_id = connection.request_ndvi(points=[[long,lat],[long,lat]...])
    data = connection.get_data(query_id)

