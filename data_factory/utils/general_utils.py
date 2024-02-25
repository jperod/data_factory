""" General Utilities for data factory """

import json
import subprocess


class Utils:
    """General Utils for data factory"""

    @staticmethod
    def get_root_path(verbose: bool = False) -> str:
        """Get the root path of the algogrowth app."""
        try:
            # Run 'git rev-parse --show-toplevel' to get the root directory
            repo_root = subprocess.check_output(
                ["git", "rev-parse", "--show-toplevel"], text=True
            ).strip()
            if verbose:
                print(
                    f"Warning! get_algogrowth_path() -> Detecting repo root @ {repo_root}."
                )
            return repo_root
        except subprocess.CalledProcessError:
            # If 'git' command fails, fallback to a default path
            return "/home/jperod/repos/algogrowth/"

    @staticmethod
    def get_config() -> dict:
        """Get the configuration settings for algogrowth app."""
        config_path = f"{Utils.get_root_path()}/config.json"
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.loads(f.read())
        return config
