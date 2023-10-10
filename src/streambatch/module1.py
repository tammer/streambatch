from datetime import datetime
import json
import pandas as pd
import requests
import time

from .savgol import savgol

savgol_qids = [] # list of qids that requested savgol so that I can construct the final dataframe

class StreambatchConnection:
    def __init__(self,api_key,use_test_api=False):
        self.api_key = api_key
        self.REQUEST_URL = "https://api.streambatch.io/async"
        self.STATUS_URL = "https://api.streambatch.io/check"
        self.READ_URL = "s3://streambatch-data"
        if use_test_api:
            print("Using test API")
            self.REQUEST_URL = "https://test.streambatch.io/async"
            self.STATUS_URL = "https://test.streambatch.io/check"
    
    def make_request(self,ndvi_request):
        response =  requests.post(self.REQUEST_URL, json=ndvi_request, headers={'X-API-Key': self.api_key})
        if response.status_code != 200:
            raise ValueError("{}".format(json.dumps(json.loads(response.text),indent=4)))
        query_id = json.loads(response.content)['id']
        access_url = json.loads(response.content)['access_url']
        return (query_id,access_url)
    
    def validate_polygon_input(self,polygons):
        space = None # this will be set below
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
        return space

    def validate_souces_input(self,sources):
        # if sources is None, then set it to a list containing one element, ndvi.streambatch
        if sources is None:
            sources = ["ndvi.streambatch_v2"]
        else:
            # if sources is not None, then it must be a list. if it is not, raise an error
            if not isinstance(sources,list):
                raise ValueError("sources must be a list")
            # if sources is not None, then it must be a list of strings. if it is not, raise an error
            valid_sources = ["ndvi.streambatch", 'ndvi.streambatch_v2', "ndvi.sentinel2","ndvi.modis","ndvi.landsat"]
            for s in sources:
                if not isinstance(s,str):
                    raise ValueError("sources must be a list of strings")
                # if sources is not a valid sources, raise an error
                if s not in valid_sources:
                    raise ValueError("Unknown source: {}. sources must be one of the following: {}".format(s,valid_sources))
        return sources
    
    def validate_point_input(self,points):
        # points must be a list of lists. if it is not, raise an error
        # !!! also should take dict of points
        # and geojson points = [{"type":"Point","coordinates":[0,0]}]
        if not isinstance(points,list):
            raise ValueError("Points must be a list")
        for point in points:
            if not isinstance(point,list):
                raise ValueError("Points must be a list of lists")
            if len(point) != 2:
                raise ValueError("Each point must have two values: longitude and latitude")
        return points

    
    # request_ndvi()
    # set query_id to a previous query id to skip the request completely. used for debugging and testing
    def request_ndvi(self,*,polygons=None,points=None,location_ids=None,aggregation="median",start_date=None,end_date=None,sources=None,query_id=None):
        if sources is None:
            sources = ["ndvi.streambatch_v2"]
        if sources == ['ndvi.savgol']:
            qid = self.request_ndvi_(sources=['ndvi.sentinel2','ndvi.landsat'],polygons=polygons,location_ids=location_ids,points=points,aggregation=aggregation,start_date=start_date,end_date=end_date,query_id=query_id)
            savgol_qids.append(qid)
            return qid
        else:
            return self.request_ndvi_(sources=sources,polygons=polygons,points=points,location_ids=location_ids,aggregation=aggregation,start_date=start_date,end_date=end_date,query_id=query_id)
    
    def request_ndvi_(self,*,polygons=None,points=None,location_ids=None,aggregation="median",start_date=None,end_date=None,sources=None,query_id=None):
        
        sources = self.validate_souces_input(sources)

        # you must set either polygons or points but not both
        if polygons is None and points is None:
            raise ValueError("You must set either polygons or points")
        if polygons is not None and points is not None:
            raise ValueError("You must set either polygons or points but not both")

        space = None # this will be set below 
        if polygons is not None:
            space = self.validate_polygon_input(polygons)

        elif points is not None:
            space = self.validate_point_input(points)
        else:
            raise ValueError("You must set either polygons or points")

        # aggregation must be either "mean" or "median". if it is not, raise an error
        if aggregation not in ["mean","median"]:
            raise ValueError("Aggregation must be either 'mean' or 'median'")
        
        # if start_date is None, then set it to 2013-01-01
        if start_date is None:
            start_date = datetime(2013,1,1)
        else:
            # if start_date is not None, then it must be a datetime object. if it is not, raise an error
            if not isinstance(start_date,datetime):
                # if it is a string, convert it to a datetime object
                if isinstance(start_date,str):
                    start_date = datetime.strptime(start_date,"%Y-%m-%d")
                else:
                    raise ValueError("start_date must be a a string yyyy-mm-dd or a datetime object")
        
        # if end_date is None, then set it to today
        if end_date is None:
            end_date = datetime.now()
        else:
            if not isinstance(end_date,datetime):
                # if it is a string, convert it to a datetime object
                if isinstance(end_date,str):
                    end_date = datetime.strptime(end_date,"%Y-%m-%d")
                else:
                    raise ValueError("end_date must be a datetime object")
            
        # if end_date is before start_date, raise an error
        if end_date < start_date:
            raise ValueError("end_date must be after start_date")

        t = {'start': start_date.strftime("%Y-%m-%d"), 'end': end_date.strftime("%Y-%m-%d"), 'unit': 'day'}
        
        if location_ids is None:
            ndvi_request = {'variable': sources, 'space': space, 'time': t, 'aggregation': aggregation}
        else:
            # location_ids must be a list of strings. if it is not, raise an error
            if not isinstance(location_ids,list):
                raise ValueError("location_ids must be a list")
            for location_id in location_ids:
                if not isinstance(location_id,str):
                    raise ValueError("location_ids must be a list of strings")
            # location_ids must be list of the same length as space. if it is not, raise an error
            if len(location_ids) != len(space):
                raise ValueError("location_ids must be a list of the same length as space")
            # each item in location_ids must be unique. if it is not, raise an error
            if len(location_ids) != len(set(location_ids)):
                raise ValueError("location_ids must be a list of unique values")
            ndvi_request = {'variable': sources, 'space': space, 'time': t, 'aggregation': aggregation, 'location_id': location_ids}
        
        if query_id is None:
            (query_id,access_url) = self.make_request(ndvi_request)
        else:
            # query_id = query_id
            access_url = f's3://streambatch-data/{query_id}.parquet'
        print("Query ID: {}".format(query_id))
        if polygons is not None:
            print("Number of polygons: {}".format(len(space)))
        elif points is not None:
            print("Number of points: {}".format(len(space)))
        print("Start date: {}".format(start_date.strftime("%Y-%m-%d")))
        print("End date: {}".format(end_date.strftime("%Y-%m-%d")))
        print("Aggregation: {}".format(aggregation))


        return query_id
    
    def get_data(self,query_id,debug=False):
        if query_id in savgol_qids:
            df = self.get_data_(query_id)
            df = savgol(df)
            if( debug == True ):
                return df
            else:
                # drop columns ndvi.sentinel2 and ndvi.landsat and ndvi.interpolated
                df = df.drop(columns=['ndvi.sentinel2','ndvi.landsat','ndvi.interpolated'])
                # rename column ndvi.savgol to ndvi
                df = df.rename(columns={"ndvi.savgol":"ndvi"})
                return df
        else:
            return self.get_data_(query_id)
    
    
    # do this step in as a separate function so that I can mock it in the tests
    def status(self,query_id):
        status_response = requests.get('{}?query_id={}'.format(self.STATUS_URL, query_id), headers={'X-API-Key': self.api_key})
        return status_response.text
    
    # do this step in as a separate function so that I can mock it in the tests
    def read_parquet(self,access_url):
        df = pd.read_parquet(access_url, storage_options={"anon": True})
        return df

    def get_data_(self,query_id):
        final_status = None
        access_url = f'{self.READ_URL}/{query_id}.parquet'
        while final_status is None:
            status = json.loads(self.status(query_id))
            if status['status'] == 'Succeeded':
                final_status = 'Succeeded'
            elif status['status'] == 'Failed':
                final_status = 'Failed'
            else:
                print(".",end="",flush=True)
                time.sleep(5)
        print("")
        if final_status == 'Failed':
            print("Error: {}".format(status)) # !!! need to parse the error message
            return None
        else:
            df = self.read_parquet(access_url)
            # !!! need to add the polygon id to the dataframe
            return df

    def query_done(self,query_id):
        status = json.loads(self.status(query_id))
        if status['status'] == 'Succeeded':
            return True
        elif status['status'] == 'Failed':
            return True
        else:
            return False
    

