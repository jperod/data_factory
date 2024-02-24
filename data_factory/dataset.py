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
