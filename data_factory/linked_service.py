"""Linked Service Module."""

from enum import Enum
from data_factory.utils.validation_utils import StringUtils


class ServiceType(Enum):
    """Enumeration representing types of services."""

    HTTP_ENDPOINT = "HttpEndpoint"
    # SQL_SERVER = "SqlServer"


class LinkedService:
    """
    Represents a linked service.

    Attributes:
        name (str): The name of the linked service.
        connection_string (str): The connection string for the linked service.
        service_type (ServiceType): The type of the linked service.
        properties (dict): Additional properties for the linked service.
    """

    def __init__(
        self,
        name: str,
        connection_string: str,
        service_type: ServiceType,
        properties: dict = None,
    ) -> None:
        """
        Initialize a linked service.

        Args:
            name (str): The name of the linked service.
            connection_string (str): The connection string for the linked service.
            service_type (ServiceType): The type of the linked service.
            properties (dict, optional): Additional properties for the linked service.

        Raises:
            ValueError: If the name is not in snake_case.
            ValueError: If the service_type is not a valid ServiceType.
        """
        if not StringUtils.validate_snake_case(name):
            raise ValueError(f"The name '{name}' is not in snake_case.")
        self.name = name
        self.connection_string = connection_string
        if not isinstance(service_type, ServiceType):
            raise ValueError(
                f"Invalid service_type. Allowed types are {', '.join(type_.value for type_ in ServiceType)}."
            )
        self.service_type = service_type
        self.properties = properties or {}
