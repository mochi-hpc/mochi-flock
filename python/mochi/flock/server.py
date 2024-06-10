# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


"""
.. module:: server
   :synopsis: This package provides access to the Flock C++ wrapper

.. moduleauthor:: Matthieu Dorier <mdorier@anl.gov>


"""


import pyflock_common
from .view import GroupView
import pyflock_server
import pymargo.core
import pymargo


class Provider:

    def __init__(self, engine: pymargo.core.Engine, provider_id: int, config: str, initial_view: GroupView):
        self._internal = pyflock_server.Provider(
            engine.get_internal_mid(), provider_id, config, initial_view)
