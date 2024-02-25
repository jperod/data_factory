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
        """
        Test initialization of the Dataset class.

        Args:
        - name (str): The name of the dataset.
        - dataset_type (DatasetType): The type of the dataset.

        Returns:
        bool: True if initialization succeeds, False otherwise.
        """
        try:
            Dataset(name, "data_loc", dataset_type)
            return True
        except Exception:  # pylint: disable=W0718
            return False

    def test_build_parametrized_data_location(self):
        """
        Test the build_parametrized_data_location method.

        Verifies that placeholders in data_location are correctly replaced with provided values.
        """

        # Create a Dataset instance with placeholders
        ds_test_params = Dataset(
            "sec_gov_api_company_facts",
            "Test/api/xbrl/companyfacts/<CIK>.json",
            DatasetType.FILE,
            parameters={"<CIK>": "cik"},
        )

        # Test with an integer value
        param_data_location = ds_test_params.build_parametrized_data_location(cik=123)
        assert param_data_location == "Test/api/xbrl/companyfacts/123.json"

        # Test with a string value
        param_data_location = ds_test_params.build_parametrized_data_location(cik="321")
        assert param_data_location == "Test/api/xbrl/companyfacts/321.json"

        # Test with a string value
        param_data_location = ds_test_params.build_parametrized_data_location(cik=None)
        assert param_data_location == "Test/api/xbrl/companyfacts/.json"
