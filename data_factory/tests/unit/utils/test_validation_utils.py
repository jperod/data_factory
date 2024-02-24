"""Test StringUtils module."""

from data_factory.utils import StringUtils


class TestStringUtils:
    """Test class for StringUtils."""

    @staticmethod
    def test_validate_snake_case() -> None:
        """
        Test the validate_snake_case function.

        Example usage:
        - Valid snake case: "valid_snake_case"
        - Invalid snake case: "Invalid_Snake_Case"

        Asserts:
        - Result should be True for valid snake case.
        - Result should be False for invalid snake case.
        """
        input_string = "valid_snake_case"
        result = StringUtils.validate_snake_case(input_string)
        assert result

        input_string = "Invalid_Snake_Case"
        result = StringUtils.validate_snake_case(input_string)
        assert not result
