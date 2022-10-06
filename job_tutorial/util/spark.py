from dependency_injector.wiring import Provide, inject
from cbor_builder.di_container import Container

@inject
def spark(session=Provide[Container.session]):
    return session
