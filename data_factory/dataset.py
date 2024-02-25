"""Module for datafactory dataset"""

from enum import Enum
from data_factory.utils.validation_utils import StringUtils


class DatasetType(Enum):
    """Dataset Types"""

    FILE = "File"
    # DATABASE = "Database"


class Dataset:
    """Dataset Class"""

    def __init__(
        self,
        name: str,
        data_location: str,
        dataset_type: DatasetType,
        properties: dict = None,
        parameters: dict = None,
    ) -> None:
        if not StringUtils.validate_snake_case(name):
            raise ValueError(f"The name '{name}' is not in snake_case.")
        self.name = name
        self.data_location = data_location
        if not isinstance(dataset_type, DatasetType):
            raise ValueError(
                f"Invalid dataset_type. Allowed types are {', '.join(type_.value for type_ in DatasetType)}."
            )

        self.dataset_type = dataset_type
        self.properties = properties or {}
        self.parameters = parameters or {}

    def build_parametrized_data_location(self, **kwargs):
        """
        Replace placeholders in data_location using key-value pairs in parameters.

        Args:
        - kwargs: Key-value pairs to replace placeholders.

        Returns:
        str: The updated data_location.
        """
        updated_data_location = self.data_location

        # Replace placeholders using provided key-value pairs
        for key, value in kwargs.items():
            placeholder = f"<{key.upper()}>"
            if placeholder in updated_data_location:
                if not value:
                    value = ""
                updated_data_location = updated_data_location.replace(
                    placeholder, str(value)
                )

        return updated_data_location
