import unittest
import json
import tempfile
import os
from mochi.flock.view import GroupView, Member, Metadata


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

    def test_members_iter(self):
        view = GroupView()
        # add 5 members
        for i in range(0, 5):
            view.members.add(
                address=f"address{i}",
                provider_id=i)
        # iterate and collect
        collected = []
        for member in view.members:
            collected.append((member.address, member.provider_id))
        # verify all members are present
        self.assertEqual(len(collected), 5)
        for i in range(0, 5):
            self.assertIn((f"address{i}", i), collected)

    def test_members_contains(self):
        view = GroupView()
        view.members.add("address1", 1)
        view.members.add("address2", 2)
        # test __contains__ with tuple
        self.assertTrue(("address1", 1) in view.members)
        self.assertTrue(("address2", 2) in view.members)
        # test non-existent members
        self.assertFalse(("address1", 2) in view.members)
        self.assertFalse(("address3", 3) in view.members)

    def test_metadata_iter(self):
        view = GroupView()
        # add 5 metadata entries
        for i in range(0, 5):
            view.metadata.add(
                key=f"key{i}",
                value=f"value{i}")
        # iterate and collect
        collected = {}
        for md in view.metadata:
            collected[md.key] = md.value
        # verify all metadata are present
        self.assertEqual(len(collected), 5)
        for i in range(0, 5):
            self.assertIn(f"key{i}", collected)
            self.assertEqual(collected[f"key{i}"], f"value{i}")

    def test_metadata_contains(self):
        view = GroupView()
        view.metadata.add("key1", "value1")
        view.metadata.add("key2", "value2")
        # test __contains__ with key
        self.assertTrue("key1" in view.metadata)
        self.assertTrue("key2" in view.metadata)
        # test non-existent keys
        self.assertFalse("key3" in view.metadata)
        self.assertFalse("nonexistent" in view.metadata)

    def test_serialize_to_file_and_from_file(self):
        view = GroupView()
        # add members and metadata
        for i in range(0, 3):
            view.members.add(f"address{i}", i)
            view.metadata.add(f"key{i}", f"value{i}")
        # serialize to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_filename = f.name
        try:
            view.serialize_to_file(temp_filename)
            # verify file exists
            self.assertTrue(os.path.exists(temp_filename))
            # load from file
            loaded_view = GroupView.from_file(temp_filename)
            # verify members
            self.assertEqual(len(loaded_view.members), 3)
            for i in range(0, 3):
                self.assertTrue((f"address{i}", i) in loaded_view.members)
            # verify metadata
            self.assertEqual(len(loaded_view.metadata), 3)
            for i in range(0, 3):
                self.assertTrue(f"key{i}" in loaded_view.metadata)
                self.assertEqual(loaded_view.metadata[f"key{i}"], f"value{i}")
            # verify digest matches
            self.assertEqual(view.digest, loaded_view.digest)
        finally:
            # clean up
            if os.path.exists(temp_filename):
                os.remove(temp_filename)

    def test_from_string(self):
        view = GroupView()
        # add members and metadata
        view.members.add("addr1", 10)
        view.members.add("addr2", 20)
        view.metadata.add("foo", "bar")
        # serialize to string
        serialized = str(view)
        # load from string
        loaded_view = GroupView.from_string(serialized)
        # verify members
        self.assertEqual(len(loaded_view.members), 2)
        self.assertTrue(("addr1", 10) in loaded_view.members)
        self.assertTrue(("addr2", 20) in loaded_view.members)
        # verify metadata
        self.assertEqual(len(loaded_view.metadata), 1)
        self.assertTrue("foo" in loaded_view.metadata)
        self.assertEqual(loaded_view.metadata["foo"], "bar")
        # verify digest matches
        self.assertEqual(view.digest, loaded_view.digest)

    def test_locked_context_manager(self):
        view = GroupView()
        view.members.add("address1", 1)
        view.metadata.add("key1", "value1")
        # use context manager
        with view.locked():
            # should be able to access view while locked
            self.assertEqual(len(view.members), 1)
            self.assertEqual(len(view.metadata), 1)
            # iterate while locked
            members_list = list(view.members)
            self.assertEqual(len(members_list), 1)
            self.assertEqual(members_list[0].address, "address1")
        # after exiting context, view should be unlocked
        # (we can verify by successfully locking again)
        with view.locked():
            self.assertTrue(("address1", 1) in view.members)

    def test_copy(self):
        view = GroupView()
        view.members.add("address1", 1)
        view.metadata.add("key1", "value1")
        # make a copy
        copied = view.copy()
        # verify copy has same content
        self.assertEqual(len(copied.members), 1)
        self.assertTrue(("address1", 1) in copied.members)
        self.assertEqual(len(copied.metadata), 1)
        self.assertTrue("key1" in copied.metadata)
        # verify modifying copy doesn't affect original
        copied.members.add("address2", 2)
        self.assertEqual(len(view.members), 1)
        self.assertEqual(len(copied.members), 2)


if __name__ == '__main__':
    unittest.main()
