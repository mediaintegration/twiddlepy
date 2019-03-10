# twiddle

`twiddle` is a Python library designed for end-to-end extract, transform and load pipline (or ETL for short). Using a mapper file,
and optional functions your data can be transformed into a better suited format.

## Features

- Extract, Transform and Load pipelines
- Multiple datasource options for extracting data
- Multiple repository options for loading data
- Support for mapping input data

## Installation

`twiddle` is available on the PyPi repository

`pip install twiddle`

## Usage

Create a runnable python file with the following code:

```python
from twiddle.config import config
from twiddle.driver import TwiddleDriver

driver = TwiddleDriver(config)
driver.process_data()
```

### User Configuration

Importing config from `twiddle.config` will import the default configuration items for each of the processes,
and will also look for a user defined configuration file on the path where the application is being run from.

### Mapper File

TODO
