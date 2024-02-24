"""Unit Tests for PipelineOrchestrator methods"""

import random
import os
from data_factory.utils import Utils
from data_factory.pipeline_orchestrator import PipelineOrchestrator
from data_factory.pipeline import Pipeline, Activity, PipelineRunStatus


def test_basic_pipeline_orchestration():
    """
    Test the basic pipeline orchestration functionality.

    This test creates three activities with random integers and orchestrates two pipelines.
    The activities write their random integers to separate files, and the test checks if the
    orchestrator runs the pipelines successfully and if the written integers match the expected values.

    Raises:
        AssertionError: If the test fails.
    """

    def example_activity(randint: int, activity_id: int) -> None:
        """
        Example Activity, defined as a Python function that performs an action.

        Args:
            randint (int): A random integer.
            activity_id (int): An identifier for the activity.

        Returns:
            None
        """
        print(f"Executing activity {activity_id} with randint: {randint}")
        test_delta_lake = Utils.get_config()["test_delta_lake"]
        test_delta_lake_path = f"{Utils.get_root_path()}/{test_delta_lake}/tests/test_basic_pipeline_orchestration/"
        test_activity_file_path = f"{test_delta_lake_path}activity{activity_id}.txt"
        os.makedirs(test_delta_lake_path, exist_ok=True)
        with open(test_activity_file_path, "w", encoding="utf-8") as f:
            f.write(str(randint))
        return None

    expected = [
        random.randint(1, 1337),
        random.randint(1, 1337),
        random.randint(1, 1337),
    ]

    # Create activities
    activity1 = Activity(
        "activity1",
        example_activity,
        parameters={"randint": expected[0], "activity_id": 1},
    )
    activity2 = Activity(
        "activity2",
        example_activity,
        parameters={"randint": expected[1], "activity_id": 2},
    )
    activity3 = Activity(
        "activity3",
        example_activity,
        parameters={"randint": expected[2], "activity_id": 3},
    )

    # Create a pipeline and add the activities
    my_pipeline_1 = Pipeline("my_pipeline_1", activities=[activity1, activity2])
    my_pipeline_2 = Pipeline("my_pipeline_2", activities=[activity3])

    pipeline_orchestrator = PipelineOrchestrator([my_pipeline_1, my_pipeline_2])
    pipeline_orchestrator.run_pipelines(verbose=True)
    pipeline_statuses = pipeline_orchestrator.get_run_statuses()

    # Check the pipeline's orchestrator run statuses
    assert list(pipeline_statuses.values()) == [
        PipelineRunStatus.SUCCEEDED for _ in range(2)
    ]

    test_delta_lake = Utils.get_config()["test_delta_lake"]
    test_delta_lake_path = f"{Utils.get_root_path()}/{test_delta_lake}/tests/test_basic_pipeline_orchestration/"
    results = []
    for activity_id in range(3):
        test_activity_file_path = f"{test_delta_lake_path}activity{activity_id+1}.txt"
        with open(test_activity_file_path, "r", encoding="utf-8") as f:
            result = int(f.read())
            print(
                f"Result_{activity_id+1} = {result} & Expected_{activity_id+1} = {expected[activity_id]}"
            )
        results.append(result)
    assert results == expected
