# (C) 2024 The University of Chicago
# See COPYRIGHT in top-level directory.


"""
.. module:: client
   :synopsis: This package provides access to the Flock C++ wrapper

.. moduleauthor:: Matthieu Dorier <mdorier@anl.gov>


"""


import pyflock_common
import pyflock_client
import pymargo.core
import pymargo


class GroupHandle:

    def __init__(self, internal, client):
        self._internal = internal
        self._client = client

    @property
    def client(self):
        return self._client

    def update(self):
        self._internal.update()

    def view(self):
        return self._internal.view


class Client:

    def __init__(self, arg):
        if isinstance(arg, pymargo.core.Engine):
            self._engine = arg
            self._owns_engine = False
        elif isinstance(arg, str):
            self._engine = pymargo.core.Engine(arg, pymargo.client)
            self._owns_engine = True
        else:
            raise TypeError(f'Invalid argument type {type(arg)}')
        self._internal = pyflock_client.Client(self._engine.get_internal_mid())

    def __del__(self):
        del self._internal
        if self._owns_engine:
            self._engine.finalize()
            del self._engine

    @property
    def mid(self):
        return self._internal.margo_instance_id

    @property
    def engine(self):
        return self._engine

    def make_group_handle(self, address: str|pymargo.core.Address, provider_id: int = 0):
        if isinstance(address, pymargo.core.Address):
            address = str(address)
        return GroupHandle(
            self._internal.make_service_handle(address=address, provider_id=provider_id),
            self)

    def make_group_handle_from_file(self, filename: str):
        return GroupHandle(
            self._internal.make_group_handle_from_file(filename=filename),
            self)

    def make_group_handle_from_serialized(self, serialized: str):
        return GroupHandle(
            self._internal.make_group_handle_from_serialized(serialized=serialized),
            self)

