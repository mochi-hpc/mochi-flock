import unittest
import json
from mochi.flock.view import GroupView


class TestGroupView(unittest.TestCase):

    def test_init(self):
        view = GroupView()

    def test_members(self):
        view = GroupView()
        # add 5 members
        for i in range(0, 5):
            view.members.add(
                address=f"address{i}",
                provider_id=i)
        # check that the count is now 5
        self.assertEqual(len(view.members), 5)
        self.assertEqual(view.members.count, 5)
        # check the members
        for i in range(0, 5):
            assert view.members.exists(f"address{i}", i)
            assert not view.members.exists(f"address{i+1}", i)
            assert not view.members.exists(f"address{i}", i+1)
            self.assertEqual(view.members[i].address, f"address{i}")
            self.assertEqual(view.members[i].provider_id, i)
        # erase member 3 via __delitem__
        del view.members[3]
        self.assertEqual(view.members.count, 4)
        assert not view.members.exists("address3", 3)
        # erase member 2 via remove(index)
        view.members.remove(2)
        self.assertEqual(view.members.count, 3)
        assert not view.members.exists("address2", 2)
        # erase member 1 via remove(address, provider_id)
        view.members.remove("address1", 1)
        self.assertEqual(view.members.count, 2)
        assert not view.members.exists("address1", 1)

    def test_metadata(self):
        view = GroupView()
        # add 5 metadata
        for i in range(0, 5):
            view.metadata.add(
                key=f"key{i}",
                value=f"value{i}")
        # check that the count is now 5
        self.assertEqual(len(view.metadata), 5)
        self.assertEqual(view.metadata.count, 5)
        # check the metadata
        for i in range(0, 5):
            self.assertEqual(view.metadata[f"key{i}"], f"value{i}")
            self.assertEqual(view.metadata[i].key, f"key{i}")
            self.assertEqual(view.metadata[i].value, f"value{i}")
        # erase key3 via __delitem__
        del view.metadata["key3"]
        self.assertEqual(view.metadata.count, 4)
        self.assertIsNone(view.metadata["key3"])
        # erase key2 via remove(key)
        view.metadata.remove("key2")
        self.assertEqual(view.metadata.count, 3)
        self.assertIsNone(view.metadata["key2"])

    def test_str(self):
        view = GroupView()
        # add 5 metadata
        for i in range(0, 5):
            view.metadata.add(
                key=f"key{i}",
                value=f"value{i}")
        view = GroupView()
        # add 5 members
        for i in range(0, 5):
            view.members.add(
                address=f"address{i}",
                provider_id=i)
        v = json.loads(str(view))
        self.assertIn("members", v)
        self.assertIn("metadata", v)
        self.assertIsInstance(v["members"], list)
        self.assertIsInstance(v["metadata"], dict)
        for i, member in enumerate(v["members"]):
            self.assertEqual(member["address"], f"address{i}")
            self.assertEqual(member["provider_id"], i)
        i = 0
        for key, val in v["metadata"]:
            self.assertEqual(key, f"key{i}")
            self.assertEqual(val, f"value{i}")
            i = i + 1

    def test_digest(self):
        view = GroupView()
        self.assertEqual(view.digest, 0)
        view.members.add("address", 1)
        self.assertNotEqual(view.digest, 0)
        d = view.digest
        view.metadata.add("key", "value")
        self.assertNotEqual(view.digest, 0)
        self.assertNotEqual(view.digest, d)

if __name__ == '__main__':
    unittest.main()
