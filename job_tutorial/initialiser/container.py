from job_tutorial import di_container
from job_tutorial.util import config, env

mods = ['job_tutorial.util.spark',
        'job_tutorial.util.configuration']

def build_container():
    if not env.Env().env == 'test':
        init_container()


def init_container():
    di = di_container.Container()
    di.config.from_dict(config.config)
    di.wire(modules=mods)
    return di


