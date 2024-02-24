"""Unit tests for the LinkedService class."""

from enum import Enum
import warnings
from data_factory.linked_service import LinkedService, ServiceType


warnings.filterwarnings("ignore")


class TestServiceType(Enum):
    """Test ServiceType(Enum)."""

    TEST = "Test"


class TestLinkedService:
    """Test Linked Services."""

    def test_init_names(self):
        """Test initialization with correct and incorrect names."""
        test_succeeded = self._test_init("name_correct", ServiceType.HTTP_ENDPOINT)
        assert test_succeeded

        test_succeeded = self._test_init("name_Incorrect", ServiceType.HTTP_ENDPOINT)
        assert not test_succeeded

    def test_init_service_type(self):
        """Test initialization with correct and incorrect service types."""
        test_succeeded = self._test_init("name_correct", TestServiceType.TEST)
        assert not test_succeeded

        test_succeeded = self._test_init("name_correct", "test")
        assert not test_succeeded

        test_succeeded = self._test_init("name_correct", ServiceType.HTTP_ENDPOINT)
        assert test_succeeded

    def _test_init(self, name: str, service_type, conn_str: str = "connstr") -> bool:
        """Common initialization test logic."""
        try:
            LinkedService(name, conn_str, service_type)
            return True
        except Exception:  # pylint: disable=W0718
            return False
