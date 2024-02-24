""" Methods for Orchestrating Pipelines """


class PipelineOrchestrator:
    """Class representing an orchestrator for managing multiple pipelines."""

    def __init__(self, pipelines: list) -> None:
        """
        Initialize PipelineOrchestrator instance.

        Parameters:
        - pipelines (list): List of pipeline instances to be orchestrated.

        Returns:
        None
        """
        self.pipelines = pipelines
        self.validate_unique_pipeline_names()

    def validate_unique_pipeline_names(self):
        """
        Validate that all pipelines have unique names.

        Raises:
        ValueError: If duplicate pipeline names are found.
        """
        pipeline_names = [pipeline.name for pipeline in self.pipelines]
        duplicate_names = set(
            name for name in pipeline_names if pipeline_names.count(name) > 1
        )

        if duplicate_names:
            raise ValueError(
                f"Duplicate pipeline names found: {', '.join(duplicate_names)}"
            )

    def run_pipelines(self, verbose: bool = False):
        """
        Run multiple pipelines sequentially.

        Parameters:
        - verbose (bool, optional): If True, print verbose information.

        Returns:
        None
        """
        for i, pipeline in enumerate(self.pipelines):
            if verbose:
                print(
                    f"\n[Orchestrator] --> ({i+1}/{len(self.pipelines)}) [Running Pipeline: {pipeline.name}]"
                )
            pipeline.run(verbose=verbose)
            if verbose:
                status = pipeline.get_run_status()
                print(
                    f"[Orchestrator] --> ({i+1}/{len(self.pipelines)}) [Pipeline: {pipeline.name}] Done with status = {status}.\n"
                )

    def get_run_statuses(self):
        """
        Get the run statuses of all pipelines.

        Returns:
        dict: Dictionary containing pipeline names as keys and their run statuses as values.
        """
        return {pipeline.name: pipeline.get_run_status() for pipeline in self.pipelines}
