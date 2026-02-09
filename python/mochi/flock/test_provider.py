import unittest
import json
import mochi.flock.server as mfs
import mochi.flock.view as view
import mochi.margo

class TestClient(unittest.TestCase):

    def test_init_provider(self):
        with mochi.margo.Engine("na+sm", mochi.margo.server) as engine:
            address = str(engine.address)
            config = {
                "group": {
                    "type": "static",
                    "config": {}
                }
            }
            initial_view = view.GroupView()
            initial_view.members.add(address, 42)
            provider = mfs.Provider(engine, 42, json.dumps(config), initial_view)
            engine.finalize()


if __name__ == '__main__':
    unittest.main()
