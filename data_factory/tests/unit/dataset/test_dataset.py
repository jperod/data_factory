"""Unit tests for the Dataset class."""

from data_factory.dataset import Dataset, DatasetType


class TestDataset:
    """Test suite for the Dataset class."""

    def test_init_names(self):
        """Test initialization with correct and incorrect names."""
        test_succeeded = self._test_init("name_correct")
        assert test_succeeded

        test_succeeded = self._test_init("name_Incorrect")
        assert not test_succeeded

    def test_init_dataset_type(self):
        """Test initialization with correct and incorrect dataset types."""
        test_succeeded = self._test_init("name_correct", dataset_type=DatasetType.FILE)
        assert test_succeeded

        test_succeeded = self._test_init("name_correct", dataset_type="File")
        assert not test_succeeded

    def _test_init(
        self, name: str, dataset_type: DatasetType = DatasetType.FILE
    ) -> bool:
        """Common initialization test logic."""
        try:
            Dataset(name, "data_loc", dataset_type)
            return True
        except Exception:  # pylint: disable=W0718
            return False
