import yaml

# Reads YAML config files, load producer configuration

def load_yaml(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)