# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


"""
.. module:: view
   :synopsis: This package provides access to the Flock C++ wrapper

.. moduleauthor:: Matthieu Dorier <mdorier@anl.gov>


"""


import pyflock_common
from contextlib import contextmanager


GroupView = pyflock_common.GroupView
Member = pyflock_common.Member
Metadata = pyflock_common.Metadata


def __members(self):
    return self._members()


def __metadata(self):
    return self._metadata()


@contextmanager
def __locked(self):
    """Context manager for safely locking/unlocking the group view.

    Usage:
        with view.locked():
            # Access view safely while locked
            for member in view.members:
                print(member.address)
    """
    self.lock()
    try:
        yield self
    finally:
        self.unlock()


GroupView.members = property(__members)
GroupView.metadata = property(__metadata)
GroupView.locked = __locked
