import unittest
import json
import mochi.flock.client as mfc
import mochi.flock.server as mfs
from mochi.flock.view import GroupView
import pymargo.core

class TestClient(unittest.TestCase):

    def setUp(self):
        self.engine = pymargo.core.Engine("na+sm", pymargo.core.server)
        self.address = str(self.engine.address)
        config = {
            "group": {
                "type": "static",
                "config": {}
            }
        }
        self.initial_view = GroupView()
        for i in range(0, 5):
            self.initial_view.members.add(self.address, i)
        self.initial_view.metadata.add("mykey", "myvalue")
        self.providers = []
        for i in range(0, 5):
            self.providers.append(
                mfs.Provider(self.engine, i, json.dumps(config), self.initial_view.copy()))
        self.client = mfc.Client(self.engine)

    def tearDown(self):
        del self.client
        del self.providers
        self.engine.finalize()
        del self.engine

    def test_view(self):
        gh = self.client.make_group_handle(self.address, 3)
        gh.update()
        view = gh.view
        self.assertIsInstance(view, GroupView)
        self.assertEqual(len(view.members), 5)
        for i in range(0, 5):
            self.assertEqual(view.members[i].address, self.address)
            self.assertEqual(view.members[i].provider_id, i)
        self.assertEqual(view.metadata["mykey"], "myvalue")
        print(view)

    def test_update(self):
        gh = self.client.make_group_handle(self.address, 3)
        gh.update()


if __name__ == '__main__':
    unittest.main()
