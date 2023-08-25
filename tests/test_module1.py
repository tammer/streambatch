import unittest

from streambatch.module1 import StreambatchConnection

#     def get_ndvi(self,polygons=None,points=None,aggregation="mean",start_date=None,end_date=None,sources=None):

class TestSimple(unittest.TestCase):

    def test_add(self):
        x = StreambatchConnection("api_key",debug=True)
        with self.assertRaises(ValueError):
            x.get_ndvi()
        with self.assertRaises(ValueError):
            x.get_ndvi(polygons=1)
        with self.assertRaises(ValueError):
            x.get_ndvi(polygons=[1])
        with self.assertRaises(ValueError):
            x.get_ndvi(polygons=[{"type":"Polygon"}])
         
if __name__ == '__main__': 
    unittest.main() 