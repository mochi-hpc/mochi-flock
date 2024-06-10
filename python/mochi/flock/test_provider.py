import unittest
import json
import mochi.flock.server as mfs
import mochi.flock.view as view
import pymargo.core

class TestClient(unittest.TestCase):

    def test_init_provider(self):
        with pymargo.core.Engine("na+sm", pymargo.core.server) as engine:
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
            del provider
            engine.finalize()


if __name__ == '__main__':
    unittest.main()
