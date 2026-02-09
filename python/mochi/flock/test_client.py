import unittest
import mochi.flock.client as mfc
from mochi.margo import Engine


class TestClient(unittest.TestCase):

    def test_init_client_from_address(self):
        client = mfc.Client("na+sm")

    def test_init_client_from_engine(self):
        with Engine("na+sm") as engine:
            client = mfc.Client(engine)


if __name__ == '__main__':
    unittest.main()
