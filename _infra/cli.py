from databricker import infra

infra.configurator()(infra_config_file="_infra/infra.toml", dist="dist")

def infra_cli():
    infra.init_cli()

