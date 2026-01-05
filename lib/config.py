"""
Configuration loader for CRIU experiments.

Loads YAML configuration files and supports environment variable substitution.
"""

import os
import yaml
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigLoader:
    """Load and validate YAML configuration with environment variable support."""

    def __init__(self, config_file: Optional[str] = None, overrides: Optional[Dict[str, Any]] = None):
        """
        Initialize configuration loader.

        Args:
            config_file: Path to YAML configuration file. If None, uses default.yaml
            overrides: Dictionary of configuration overrides
        """
        self.config_file = config_file
        self.overrides = overrides or {}
        self.config = {}

    def load(self) -> Dict[str, Any]:
        """
        Load configuration from file and apply overrides.

        Returns:
            Dictionary containing complete configuration
        """
        # Load base configuration
        if self.config_file:
            config_path = Path(self.config_file)
        else:
            # Default to config/default.yaml
            base_dir = Path(__file__).parent.parent
            config_path = base_dir / 'config' / 'default.yaml'

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Substitute environment variables
        self.config = self._substitute_env_vars(self.config)

        # Apply overrides
        self.config = self._apply_overrides(self.config, self.overrides)

        return self.config

    def _substitute_env_vars(self, config: Any) -> Any:
        """
        Recursively substitute environment variables in configuration.

        Supports ${VAR_NAME} or $VAR_NAME syntax.

        Args:
            config: Configuration dictionary or value

        Returns:
            Configuration with environment variables substituted
        """
        if isinstance(config, dict):
            return {k: self._substitute_env_vars(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_env_vars(item) for item in config]
        elif isinstance(config, str):
            # Replace ${VAR} or $VAR with environment variable
            if '$' in config:
                # Handle ${VAR_NAME} format
                import re
                pattern = r'\$\{([^}]+)\}|\$([A-Za-z_][A-Za-z0-9_]*)'

                def replace_env(match):
                    var_name = match.group(1) or match.group(2)
                    return os.getenv(var_name, match.group(0))

                return re.sub(pattern, replace_env, config)
        return config

    def _apply_overrides(self, config: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply configuration overrides using dot notation.

        Args:
            config: Base configuration
            overrides: Override values (supports dot notation keys like 'checkpoint.strategy.mode')

        Returns:
            Configuration with overrides applied
        """
        result = config.copy()

        for key, value in overrides.items():
            # Support dot notation for nested keys
            if '.' in key:
                parts = key.split('.')
                current = result
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[parts[-1]] = value
            else:
                result[key] = value

        return result

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value by key (supports dot notation).

        Args:
            key: Configuration key (e.g., 'checkpoint.strategy.mode')
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        if not self.config:
            self.load()

        parts = key.split('.')
        current = self.config

        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default

        return current


class ConfigValidator:
    """Validate configuration against required schema."""

    REQUIRED_FIELDS = {
        'experiment': ['name', 'workload_type'],
        'checkpoint': ['strategy', 'dirs'],
        'transfer': ['method'],
        'nodes': ['source', 'destination'],
    }

    @staticmethod
    def validate(config: Dict[str, Any]) -> bool:
        """
        Validate configuration has all required fields.

        Args:
            config: Configuration dictionary

        Returns:
            True if valid

        Raises:
            ValueError: If required fields are missing
        """
        for section, fields in ConfigValidator.REQUIRED_FIELDS.items():
            if section not in config:
                raise ValueError(f"Missing required configuration section: {section}")

            for field in fields:
                if field not in config[section]:
                    raise ValueError(f"Missing required field: {section}.{field}")

        return True
