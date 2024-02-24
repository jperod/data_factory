"""Validation Utils Module"""

import re

class StringUtils:
    """Utility class for string validation."""

    @staticmethod
    def validate_snake_case(string: str) -> bool:
        """
        Validates if a string follows the snake_case naming convention.

        Args:
            string (str): The string to be validated.

        Returns:
            bool: True if the string is in snake_case, False otherwise.
        """
        # Use a regular expression to check if the string is in snake_case
        # Snake case allows lowercase letters, numbers, and underscores, and it should not start or end with an underscore.
        pattern = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")
        return bool(pattern.match(string))
