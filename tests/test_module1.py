import unittest
from unittest.mock import MagicMock
from datetime import datetime


from streambatch.module1 import StreambatchConnection

class TestStreambatchConnection(unittest.TestCase):
    
    def setUp(self):
        self.connection = StreambatchConnection("your_api_key")

    # Polygon validation
    
    def test_validate_polygon_input_valid_list(self):
        polygons = [
            {"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]},
            {"type": "Polygon", "coordinates": [[-1, -1], [-1, 0], [0, 0], [0, -1], [-1, -1]]}
        ]
        result = self.connection.validate_polygon_input(polygons)
        self.assertEqual(result, polygons)
    
    def test_validate_polygon_input_valid_dict(self):
        polygons = {
            "polygon1": {"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]},
            "polygon2": {"type": "Polygon", "coordinates": [[-1, -1], [-1, 0], [0, 0], [0, -1], [-1, -1]]}
        }
        expected_result = list(polygons.values())
        result = self.connection.validate_polygon_input(polygons)
        self.assertEqual(result, expected_result)
    
    def test_validate_polygon_input_invalid_type(self):
        with self.assertRaises(ValueError):
            self.connection.validate_polygon_input("not_a_list_nor_dict")
    
    def test_validate_polygon_input_invalid_list(self):
        polygons = [
            {"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]},
            "invalid_polygon"
        ]
        with self.assertRaises(ValueError):
            self.connection.validate_polygon_input(polygons)
    
    def test_validate_polygon_input_invalid_dict(self):
        polygons = {
            "polygon1": {"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]},
            "polygon2": "invalid_polygon"
        }
        with self.assertRaises(ValueError):
            self.connection.validate_polygon_input(polygons)
    
    def test_validate_polygon_input_missing_keys(self):
        polygons = [
            {"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0]]},  # Missing "coordinates"
            {"coordinates": [[-1, -1], [-1, 0], [0, 0], [0, -1], [-1, -1]]}    # Missing "type"
        ]
        with self.assertRaises(ValueError):
            self.connection.validate_polygon_input(polygons)
    
    def test_validate_sources_input_valid_default(self):
        sources = None
        expected_result = ["ndvi.streambatch_v2"]
        result = self.connection.validate_souces_input(sources)
        self.assertEqual(result, expected_result)
    
    def test_validate_sources_input_valid_custom(self):
        sources = ["ndvi.sentinel2", "ndvi.modis"]
        result = self.connection.validate_souces_input(sources)
        self.assertEqual(result, sources)
    
    def test_validate_sources_input_invalid_type(self):
        with self.assertRaises(ValueError):
            self.connection.validate_souces_input("not_a_list")
    
    def test_validate_sources_input_invalid_values(self):
        sources = ["ndvi.sentinel2", "invalid_source"]
        with self.assertRaises(ValueError):
            self.connection.validate_souces_input(sources)
    
    def test_validate_sources_input_invalid_values_mixed(self):
        sources = ["ndvi.sentinel2", 123]
        with self.assertRaises(ValueError):
            self.connection.validate_souces_input(sources)

    def test_valid_sources_v2(self):
        sources = ["ndvi.streambatch_v2"]
        result = self.connection.validate_souces_input(sources)
        self.assertEqual(result, sources)

    # Point validation
    
    def test_validate_point_input_valid(self):
        points = [[0, 0], [1, 1], [2, 2]]
        result = self.connection.validate_point_input(points)
        self.assertEqual(result, points)
    
    # Not implemented yet
    # def test_validate_point_input_valid_geojson(self):
    #     points = [{"type": "Point", "coordinates": [0, 0]}, {"type": "Point", "coordinates": [1, 1]}]
    #     expected_result = [[0, 0], [1, 1]]  # Converted to regular coordinates
    #     result = self.connection.validate_point_input(points)
    #     self.assertEqual(result, expected_result)
    
    def test_validate_point_input_invalid_type(self):
        with self.assertRaises(ValueError):
            self.connection.validate_point_input("not_a_list")
    
    def test_validate_point_input_invalid_sublist(self):
        points = [[0, 0], "invalid_point"]
        with self.assertRaises(ValueError):
            self.connection.validate_point_input(points)
    
    def test_validate_point_input_invalid_sublist_length(self):
        points = [[0, 0], [1]]  # Should have two values
        with self.assertRaises(ValueError):
            self.connection.validate_point_input(points)

    # request_ndvi() validation
    
    def test_request_ndvi_invalid_input(self):
        with self.assertRaises(ValueError):
            self.connection.request_ndvi_()  # No polygons or points
    
    def test_request_ndvi_invalid_aggregation(self):
        polygons = [{"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]}]
        sources = ["ndvi.sentinel2"]
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 8, 31)
        
        with self.assertRaises(ValueError):
            self.connection.request_ndvi_(polygons=polygons, sources=sources, start_date=start_date, end_date=end_date, aggregation="invalid")
    
    def test_request_ndvi_valid_dates(self):
        polygons = [{"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]}]
        sources = ["ndvi.sentinel2"]
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 8, 31)
        
        with self.assertRaises(ValueError):
            self.connection.request_ndvi_(polygons=polygons, sources=sources, start_date=end_date, end_date=start_date)
    
    def test_request_ndvi_valid(self):
        polygons = [{"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]}]
        sources = ["ndvi.sentinel2"]
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 8, 31)

        self.connection.make_request = MagicMock(return_value=("1","2"))
        
        query_id = self.connection.request_ndvi_(polygons=polygons, sources=sources, start_date=start_date, end_date=end_date)
        self.assertEqual(query_id, "1")

    def test_request_ndvi_bad_api_key(self):
        polygons = [{"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]}]
        with self.assertRaises(ValueError):
            self.connection.request_ndvi_(polygons=polygons)

    def test_request_ndvi_with_location_ids_wrong_number(self):
        location_ids = ["1", "2"]
        polygons = [{"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]}]
        sources = ["ndvi.sentinel2"]
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 8, 31)
        with self.assertRaises(ValueError):
            self.connection.request_ndvi_(polygons=polygons, location_ids=location_ids, sources=sources, start_date=start_date, end_date=end_date)

    def test_request_ndvi_with_location_ids_not_unique(self):
        location_ids = ["1", "1"]
        polygons = [{"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]}]
        sources = ["ndvi.sentinel2"]
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 8, 31)
        with self.assertRaises(ValueError):
            self.connection.request_ndvi_(polygons=polygons, location_ids=location_ids, sources=sources, start_date=start_date, end_date=end_date)  

    def test_request_ndvi_with_location_ids_wrong_type(self):
        location_ids = ["1", 1]
        polygons = [{"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]}]
        sources = ["ndvi.sentinel2"]
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 8, 31)
        with self.assertRaises(ValueError):
            self.connection.request_ndvi_(polygons=polygons, location_ids=location_ids, sources=sources, start_date=start_date, end_date=end_date)

    def test_request_ndvi_with_location_ids_valid(self):
        location_ids = ["1"]
        polygons = [{"type": "Polygon", "coordinates": [[0, 0], [0, 1], [1, 1], [1, 0]]}]
        sources = ["ndvi.sentinel2"]
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 8, 31)

        self.connection.make_request = MagicMock(return_value=("1","2"))
        
        query_id = self.connection.request_ndvi_(polygons=polygons, location_ids=location_ids, sources=sources, start_date=start_date, end_date=end_date)
        self.assertEqual(query_id, "1")

    # !!! TODO: test that sources defaults to what you want it to; same for aggregation

    # test get data

    def test_get_data_1(self):
        self.connection.status = MagicMock(return_value='{"status":"Failed"}')
        self.assertIsNone(self.connection.get_data("1"))
    
    def test_get_data_2(self):
        self.connection.status = MagicMock(return_value='{"status":"Succeeded"}')
        self.connection.read_parquet = MagicMock(return_value='12345')
        self.assertEqual(self.connection.get_data("1"), '12345')
    
    def test_get_data_3(self):
        self.connection.status = MagicMock()
        self.connection.status.side_effect = ['{"status":"Running"}', '{"status":"Succeeded"}']
        self.connection.read_parquet = MagicMock(return_value='12345')
        self.assertEqual(self.connection.get_data("1"), '12345')

    def test_query_done(self):
        self.connection.status = MagicMock(return_value='{"status":"Failed"}')
        self.assertTrue(self.connection.query_done("1"))
        self.connection.status = MagicMock(return_value='{"status":"Succeeded"}')
        self.assertTrue(self.connection.query_done("1"))
        self.connection.status = MagicMock(return_value='{"status":"Running"}')
        self.assertFalse(self.connection.query_done("1"))
    
    # misc tests (old)
    def test_add(self):
        x = StreambatchConnection("api_key")
        with self.assertRaises(ValueError):
            x.request_ndvi()
        with self.assertRaises(ValueError):
            x.request_ndvi(polygons=1)
        with self.assertRaises(ValueError):
            x.request_ndvi(polygons=[1])
        with self.assertRaises(ValueError):
            x.request_ndvi(polygons=[{"type":"Polygon"}])    
         
if __name__ == '__main__': 
    unittest.main() 