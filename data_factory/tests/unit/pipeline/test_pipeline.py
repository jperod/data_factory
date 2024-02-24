"""Test Pipeline and Activity Class."""

from data_factory.pipeline import Pipeline, Activity, PipelineRunStatus


def test_basic_pipeline_flow():
    """
    Test the basic flow of a pipeline.

    - Create three activities.
    - Create a pipeline and add the activities.
    - Run the pipeline.
    - Check that the pipeline's run status is 'SUCCEEDED'.
    """

    def example_activity(parameter):
        """Example Activity, defined as a python function that performs an action."""
        print(f"Executing activity with parameter: {parameter}")

    # Create activities
    activity1 = Activity(
        "activity1", example_activity, parameters={"parameter": "value1"}
    )
    activity2 = Activity(
        "activity2", example_activity, parameters={"parameter": "value2"}
    )
    activity3 = Activity(
        "activity3", example_activity, parameters={"parameter": "value3"}
    )

    # Create a pipeline and add the activities
    my_pipeline = Pipeline("my_pipeline", activities=[activity1, activity2, activity3])

    assert my_pipeline.get_run_status() is None

    # Run the pipeline
    my_pipeline.run()

    # Check the pipeline's run status
    assert my_pipeline.get_run_status() == PipelineRunStatus.SUCCEEDED


# def test_activity_timeout():
#     def wait_longer_than_timeout(_timeout_seconds:int):

#         # time_st = time.time()
#         # while time.time() - time_st < _timeout_seconds:
#         #     print(f"waiting {5} sec with timeout = {_timeout_seconds}...")
#         #     time.sleep(5)
#     # Set up the timeout signal
#         signal.signal(signal.SIGALRM, timeout_handler)
#         signal.alarm(_timeout_seconds)  # Set the timeout in seconds

#         try:
#             time_st = time.time()
#             while time.time() - time_st < _timeout_seconds:
#                 print(f"waiting {5} sec with timeout = {_timeout_seconds}...")
#                 time.sleep(5)
#         finally:
#             # Reset the alarm
#             signal.alarm(0)

#     # Create activities
#     activity_to_timeout = Activity("activity_to_timeout", wait_longer_than_timeout, timeout_seconds=0.1)

#     # Run the pipeline
#     activity_to_timeout.run()

#     # Check the pipeline's run status
#     print(activity_to_timeout.get_run_status())
#     print(activity_to_timeout.get_failure_reason())
