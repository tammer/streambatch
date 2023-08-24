from datetime import datetime
import requests

REQUEST_URL = "https://api.streambatch.io/async"

class StreambatchConnection:
    def __init__(self,api_key,syncronous=True):
        self.api_key = api_key
        self.syncronous = syncronous
    
    def get_ndvi(self,polygons=None,points=None,aggregation="mean",start_date=None,end_date=None,sources=None):
        
        # if sources is None, then set it to a list containing one element, ndvi.streambatch
        if sources is None:
            sources = ["ndvi.streambatch"]
        else:
            # if sources is not None, then it must be a list. if it is not, raise an error
            if not isinstance(sources,list):
                raise ValueError("sources must be a list")
            # if sources is not None, then it must be a list of strings. if it is not, raise an error
            valid_sources = ["ndvi.streambatch","ndvi.sentinel2","ndvi.modis","ndvi.landast"]
            for s in sources:
                if not isinstance(s,str):
                    raise ValueError("sources must be a list of strings")
                # if sources is not a valid sources, raise an error
                if s not in valid_sources:
                    raise ValueError("sources must be one of the following: {}".format(valid_sources))

        # you must set either polygons or points but not both
        if polygons is None and points is None:
            raise ValueError("You must set either polygons or points")
        if polygons is not None and points is not None:
            raise ValueError("You must set either polygons or points but not both")

        space = None # this will be set below 
        if polygons is not None:
            # polytons must either be a list or a dict. if it is not one of those, raise an error
            if not isinstance(polygons,dict) and not isinstance(polygons,list):
                raise ValueError("Polygons must be a list or a dict")
            # if polygons is a list, it must be a list of dicts. if it is not, raise an error
            if isinstance(polygons,list):
                for polygon in polygons:
                    if not isinstance(polygon,dict):
                        raise ValueError("Polygons must be a list of dicts")
                    # polygon must have two keys: "type" and "coordinates". if it does not, raise an error
                    if "type" not in polygon or "coordinates" not in polygon:
                        raise ValueError("Each polygon must have a 'type' and 'coordinates' key")
                space = polygons
            # if polygons is a dict, it must have, each value must be a dics. if it is not, raise an error
            if isinstance(polygons,dict):
                for key,value in polygons.items():
                    if not isinstance(value,dict):
                        raise ValueError("Polygons must be a dict where the key is an id and value is a polygon")
                    # each value must have two keys: "type" and "coordinates". if it does not, raise an error
                    if "type" not in value or "coordinates" not in value:
                        raise ValueError("Each polygon must have a 'type' and 'coordinates' key")
                # space is the iist of values from the dict
                space = list(polygons.values())
            print("Number of polygons: {}".format(len(space)))

        if points is not None:
            # points must be a list of lists. if it is not, raise an error
            if not isinstance(points,list):
                raise ValueError("Points must be a list")
            for point in points:
                if not isinstance(point,list):
                    raise ValueError("Points must be a list of lists")
                if len(point) != 2:
                    raise ValueError("Each point must have two values: longitude and latitude")
            space = points
            print("Number of points: {}".format(len(space)))

        # aggregation must be either "mean" or "median". if it is not, raise an error
        if aggregation not in ["mean","median"]:
            raise ValueError("Aggregation must be either 'mean' or 'median'")
        
        # if start_date is None, then set it to 2013-01-01
        if start_date is None:
            start_date = datetime(2013,1,1)
        else:
            # if start_date is not None, then it must be a datetime object. if it is not, raise an error
            if not isinstance(start_date,datetime):
                raise ValueError("start_date must be a datetime object")
        
        # if end_date is None, then set it to today
        if end_date is None:
            end_date = datetime.now()
        else:
            # if end_date is not None, then it must be a datetime object. if it is not, raise an error
            if not isinstance(end_date,datetime):
                raise ValueError("end_date must be a datetime object")
            
        # if end_date is before start_date, raise an error
        if end_date < start_date:
            raise ValueError("end_date must be after start_date")
        
        #if syncronous is False, the fail
        if not self.syncronous:
            raise ValueError("Asynchronous calls are not supported yet")
        time = {'start': start_date.strftime("%Y-%m-%d"), 'end': end_date.strftime("%Y-%m-%d"), 'unit': 'day'}
        ndvi_request = {'variable': sources, 'space': space, 'time': time, 'aggregation': aggregation}
        response = requests.post(REQUEST_URL, json=ndvi_request, headers=self.api_header)
        if response.status_code != 200:
            raise ValueError("Error: {}".format(response.text))
        query_id = json.loads(response.content)['id']
        access_url = json.loads(response.content)['access_url']
        print("Query ID: {}".format(self.query_id))
