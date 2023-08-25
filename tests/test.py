import sys
from pathlib import Path

sys.path.append(
    str(Path(__file__).parent.parent.joinpath("src"))
)

from streambatch.module1 import StreambatchConnection

api_key = "1234567890"
connection = StreambatchConnection(api_key,syncronous=True)