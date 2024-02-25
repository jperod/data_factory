""""Modules for Pipeline and Activity"""

from enum import Enum
from typing import Any
from data_factory.utils.validation_utils import StringUtils


class PipelineRunStatus(Enum):
    """Enum representing the possible run statuses of a pipeline."""

    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELED = "Canceled"
    RUNNING = "Running"


class PipelineFailureReason(Enum):
    """Enum representing the possible failure reasons of a pipeline."""

    ACTIVITY_FAILED = "One or more activities failed"
    TIMEOUT = "Pipeline execution timed out"
    INVALID_CONFIGURATION = "Invalid pipeline configuration"


class SupportedPreviousActivityOutcomeVariable(Enum):
    """Enum representing possible variable names to be propagated forward from past activities"""

    DF_SPARK = "df_spark"


class Activity:
    """Class representing an activity in a pipeline."""

    def __init__(
        self,
        name: str,
        action: callable,
        timeout_seconds: int = 3600,
        parameters: dict = None,
    ) -> None:
        """
        Initialize Activity instance.

        Parameters:
        - name (str): The name of the activity.
        - action (callable): The Python function to be executed as the activity.
        - timeout_seconds (int): The maximum time (in seconds) the activity is allowed to run before timing out.
        - parameters (dict): The parameters to be passed to the activity.

        Returns:
        None
        """
        if not StringUtils.validate_snake_case(name):
            raise ValueError(f"The name '{name}' is not in snake_case.")
        self.name = name
        self.action = action
        self.timeout_seconds = timeout_seconds
        self.parameters = parameters or {}
        self.run_status = None
        self.failure_reason = None

    def _handle_timeout(self, signum, frame):
        """Handler for activity execution timeout."""
        raise TimeoutError("Activity execution timed out")

    def run(self):
        """
        Run the activity.

        Executes the associated Python function (activity) and updates the run status and failure reason.

        Returns:
        None
        """
        try:
            self.run_status = PipelineRunStatus.RUNNING
            result = self.action(**self.parameters)
            self.run_status = PipelineRunStatus.SUCCEEDED
            return result

        except Exception as e:  # pylint: disable=W0718
            self.run_status = PipelineRunStatus.FAILED
            self.failure_reason = str(e)
            return None

    def get_run_status(self):
        """
        Get the run status of the activity.

        Returns:
        str: The run status.
        """
        return self.run_status

    def get_failure_reason(self):
        """
        Get the failure reason of the activity.

        Returns:
        str: The failure reason.
        """
        return self.failure_reason


class Pipeline:
    """Class representing a pipeline."""

    def __init__(
        self, name: str, activities: list = None, parameters: dict = None
    ) -> None:
        """
        Initialize Pipeline instance.

        Parameters:
        - name (str): The name of the pipeline.
        - activities (list): List of activities in the pipeline.

        Returns:
        None
        """
        if not StringUtils.validate_snake_case(name):
            raise ValueError(f"The name '{name}' is not in snake_case.")
        self.name = name
        self.activities = activities or []
        self.parameters = parameters or {}
        self.run_status = None
        self.failure_reason = None
        self.failure_message = None

    def add_activity(self, activity: Activity):
        """
        Add an activity to the pipeline.

        Parameters:
        - activity (Activity): The activity to be added.

        Returns:
        None
        """
        self.activities.append(activity)

    def run(self, verbose: bool = False):
        """
        Run the pipeline.

        Executes each activity in the pipeline, passing the output of the previous activity
        as parameters to the current activity. Updates the run status and failure reason.

        Returns:
            None
        """
        self.run_status = PipelineRunStatus.RUNNING
        if verbose:
            print(f"\n[Pipeline: {self.name}] -> Starting...]")
        previous_output = {}

        for i, activity in enumerate(self.activities):
            if verbose:
                print(
                    f"[Pipeline: {self.name}] --> ({i+1}/{len(self.activities)}) [Activity: {activity.name}] Initializing..."
                )
            if previous_output:
                self._validate_previous_output(previous_output)
                # Update the previous_output variable with the current activity's output, currently supported values
                vars_from_previous_output = [
                    str(var.value)
                    for var in SupportedPreviousActivityOutcomeVariable
                    if var.value in previous_output
                ]
                if verbose:
                    print(f"vars_from_previous_output={vars_from_previous_output}")
                # If exists, Instantiate new activity with parameters from previous
                if len(vars_from_previous_output) > 0:
                    for _, var_prev_out in enumerate(vars_from_previous_output):
                        if verbose:
                            print(
                                f"[Pipeline: {self.name}] --> ({i+1}/{len(self.activities)}) [Activity: {activity.name}] Inheriting output variable from past activity run: {var_prev_out}]"
                            )
                        activity.parameters[var_prev_out] = previous_output[
                            var_prev_out
                        ]
                        if verbose:
                            print(
                                f"[Pipeline: {self.name}] --> ({i+1}/{len(self.activities)}) [Activity: {activity.name}] activity.parameters = {activity.parameters}]"
                            )

            # If exists, parameter variable that also exists in activity variable is passed through to activity level for execution
            params_in_act_params = [
                param
                for param in list(self.parameters.keys())
                if param in list(activity.parameters.keys())
            ]
            if len(params_in_act_params) > 0:
                for param in params_in_act_params:

                    if verbose:
                        print(
                            f"[Pipeline: {self.name}] --> ({i+1}/{len(self.activities)}) [Activity: {activity.name}] Inheriting parameter from pipeline into activity: {param}]"
                        )
                    # Overwrite activity param with param inherited from pipeline
                    activity.parameters[param] = self.parameters[param]
                    if verbose:
                        print(
                            f"[Pipeline: {self.name}] --> ({i+1}/{len(self.activities)}) [Activity: {activity.name}] activity.parameters = {activity.parameters}]"
                        )

            if verbose:
                print(
                    f"[Pipeline: {self.name}] --> ({i+1}/{len(self.activities)}) [Activity: {activity.name}] Starting..."
                )

            previous_output = activity.run()

            if verbose:
                status = activity.get_run_status()
                print(
                    f"[Pipeline: {self.name}] --> ({i+1}/{len(self.activities)}) [Activity: {activity.name}] Done with status = {status}."
                )

            if activity.get_run_status() == PipelineRunStatus.FAILED:
                self.run_status = PipelineRunStatus.FAILED
                self.failure_reason = PipelineFailureReason.ACTIVITY_FAILED
                self.failure_message = activity.get_failure_reason()
                if verbose:
                    status = self.get_run_status()
                    print(f"\n[Pipeline: {self.name}] -> Failed!!! ")
                    print(
                        f"[Pipeline: {self.name}] -> failure_reason={self.failure_reason} "
                    )
                    print(
                        f"[Pipeline: {self.name}] -> failure_message={self.failure_message} \n"
                    )
                break
        else:
            self.run_status = PipelineRunStatus.SUCCEEDED

        if verbose:
            status = self.get_run_status()
            print(f"[Pipeline: {self.name}] -> Done with status = {status}.]\n")

    def get_run_status(self):
        """
        Get the run status of the pipeline.

        Returns:
        str: The run status.
        """
        return self.run_status

    def get_failure_reason(self):
        """
        Get the failure reason of the pipeline run.

        Returns:
        str: The failure reason.
        """
        return self.failure_reason

    def get_failure_message(self):
        """
        Get the failure message of the pipeline run.

        Returns:
        str: The failure message.
        """
        return self.failure_message

    def _validate_previous_output(self, previous_output: Any) -> bool:
        """
        Validate the keys of the 'previous_output' dictionary.

        Args:
        - previous_output (dict): The dictionary to be validated.

        Raises:
        - ValueError: If the keys are not valid according to the enum.
        """
        valid_keys = {var.value for var in SupportedPreviousActivityOutcomeVariable}

        if not isinstance(previous_output, dict):
            raise ValueError(f"Previous output must be a dictionary. current previous_output = {previous_output}")

        for key in previous_output.keys():
            if key not in valid_keys:
                raise ValueError(
                    f"Invalid key in previous_output: {key}. Allowed keys are: {valid_keys}."
                )
