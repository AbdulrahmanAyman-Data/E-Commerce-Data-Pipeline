import os
import yaml
from pathlib import Path
from typing import Any

def load_config() -> dict[str, Any]:
    env = os.getenv("ENV", "dev")
    config_dir = Path(__file__).resolve().parents[3] / "config"
    base_path = config_dir / "base_config.yaml"
    env_path = config_dir / f"{env}_config.yaml"

    with open(base_path, "r") as f :
        config = yaml.safe_load(f)

    if env_path.exists():
        with open(env_path, "r")as f :
            env_config = yaml.safe_load(f) or {}
        config = _deep_merge(config, env_config)

    return config

def _deep_merge(base: dict, override: dict) -> dict :
    result = base.copy()

    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value

    return result


if __name__ == "__main__":
    config = load_config()
    import json
    print(json.dumps(config, indent=2))
            
            