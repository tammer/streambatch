import unittest

from streambatch.module1 import StreambatchConnection


class TestSimple(unittest.TestCase):

    def test_add(self):
        self.assertEqual(11, 11)


if __name__ == '__main__':
    unittest.main()

