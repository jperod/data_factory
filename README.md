# Data Factory

[![Build Status](https://github.com/jperod/data_factory/actions/workflows/ci-pylint-pytest.yml/badge.svg)](https://github.com/jperod/data_factory/actions)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://opensource.org/licenses/MIT)

Data Factory is my attempy at building my own Python library for building data pipelines with ease for my personal projects, inspired on azure data factory.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Getting Started](#examples)
- [License](#license)

## Features

- Easy-to-use Python API for defining and executing data pipelines.
- Support for defining activities and orchestrating them in a pipeline.

## Installation

You can install Data Factory using poetry:

```bash
poetry install
```

To validate the install works you can run unit tests
```py
pytest
```

## Examples

### Activities and Pipelines
```py
from data_factory.pipeline import Pipeline, Activity

# Define your activities
def activity1():
    print("Executing Activity 1")

def activity2():
    print("Executing Activity 2")

# Create activities
activity_1 = Activity("Activity 1", activity1)
activity_2 = Activity("Activity 2", activity2)

# Create a pipeline and add activities
my_pipeline = Pipeline("My Pipeline", activities=[activity_1, activity_2])

# Run the pipeline
my_pipeline.run()

```
### Orchestrating Pipelines
```py
from data_factory.orchestrator import PipelineOrchestrator

# Create pipelines
pipeline_1 = Pipeline("Pipeline 1", activities=[activity1, activity2])
pipeline_2 = Pipeline("Pipeline 2", activities=[activity3])

# Create an orchestrator and add pipelines
orchestrator = PipelineOrchestrator([pipeline_1, pipeline_2])

# Run all pipelines sequentially
orchestrator.run_pipelines(verbose=True)

# Get the run statuses of all pipelines
pipeline_statuses = orchestrator.get_run_statuses()
print(pipeline_statuses)
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
