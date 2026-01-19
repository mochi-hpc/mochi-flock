# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


"""
.. module:: view
   :synopsis: This package provides access to the Flock C++ wrapper

.. moduleauthor:: Matthieu Dorier <mdorier@anl.gov>


"""


import pyflock_common


GroupView = pyflock_common.GroupView
Member = pyflock_common.Member

def __members(self):
    return self._members()

def __metadata(self):
    return self._metadata()

GroupView.members = property(__members)
GroupView.metadata = property(__metadata)
