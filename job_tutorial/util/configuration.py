from typing import List
from dependency_injector.wiring import Provide, inject

from cbor_builder.di_container import Container
from cbor_builder.util import fn

def config_for(elements: List):
    return fn.deep_get(di_config(), elements)

@inject
def di_config(cfg=Provide[Container.config]):
    return cfg
