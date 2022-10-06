from typing import List, Dict, AnyStr, Union
from dependency_injector.wiring import Provide, inject

from job_tutorial.di_container import Container
from job_tutorial.util import fn

def config_for(elements: List) -> Union[Dict, AnyStr]:
    return fn.deep_get(di_config(), elements)

@inject
def di_config(cfg=Provide[Container.config]) -> Dict:
    return cfg
