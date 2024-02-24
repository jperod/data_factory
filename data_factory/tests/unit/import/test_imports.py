"""Test Imports for data factory tests."""


class ImportTest:
    """Class to facilitate testing imports."""

    def __init__(self, import_statement: str, import_description: str) -> None:
        """
        Initialize ImportTest instance.

        Parameters:
        - import_statement (str): The import statement to be tested.
        - import_description (str): A description of the import being tested.

        Returns:
        None
        """
        self.import_statement = import_statement
        self.import_description = import_description

    def import_test(self) -> bool:
        """
        Helper function to test imports.

        Executes the import statement and checks for syntax errors.

        Returns:
        bool: True if the import is successful, False otherwise.
        """
        try:
            compile(self.import_statement, "<string>", "exec")
            return True
        except SyntaxError:
            return False


def test_import_df():
    """
    Test general import for data factory.

    Checks if the module 'data_factory' can be imported.
    """
    import_tester = ImportTest("import data_factory", "data_factory")
    assert import_tester.import_test()


def test_import_df_linked_service():
    """
    Test import for linked service in data factory.

    Checks if the module 'linked_service' can be imported from 'data_factory'.
    """
    import_tester = ImportTest(
        "from data_factory import linked_service", "linked_service"
    )
    assert import_tester.import_test()


def test_import_df_utils_string_utils():
    """
    Test import for string utilities in data factory utils.

    Checks if 'StringUtils' can be imported from 'data_factory.utils'.
    """
    import_tester = ImportTest(
        "from data_factory.utils import StringUtils", "StringUtils"
    )
    assert import_tester.import_test()


def test_import_df_dataset():
    """
    Test import for dataset in data factory.

    Checks if the module 'dataset' can be imported from 'data_factory'.
    """
    import_tester = ImportTest("from data_factory import dataset", "dataset")
    assert import_tester.import_test()


def test_import_df_pipeline():
    """
    Test import for pipeline in data factory.

    Checks if the module 'pipeline' can be imported from 'data_factory'.
    """
    import_tester = ImportTest("from data_factory import pipeline", "pipeline")
    assert import_tester.import_test()
