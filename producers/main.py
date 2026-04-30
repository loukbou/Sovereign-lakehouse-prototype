import importlib
import logging
import os

from common.config_loader import load_yaml

# Reads config and launches the correct producer
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

CONFIG_PATH = os.environ["PRODUCER_CONFIG"] # container env.


def main():
    config = load_yaml(CONFIG_PATH)

    module_path = config["producer_module"]
    module = importlib.import_module(module_path)

    module.run(config)


if __name__ == "__main__":
    main()