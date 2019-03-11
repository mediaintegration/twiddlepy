# twiddle

`twiddle` is a Python library designed for end-to-end extract, transform and load pipline (or ETL for short). Using a mapper file,
and optional functions your data can be transformed into a better suited format.

## Features

- Extract, Transform and Load pipelines
- Multiple datasource options for extracting data
- Multiple repository options for loading data
- Support for mapping input data

## Installation

Twiddle is available on the PyPi repository

`pip install twiddle`

Or if you want to install directly from the repository: `python setup.py install`, or drop the twiddle directory anywhere on your PYTHONPATH.

## Usage

Create a runnable python file with the following code:

```python
from twiddle.config import config
from twiddle.driver import TwiddleDriver

driver = TwiddleDriver(config)
driver.process_data()
```

### Example Project Structure

```
.
|-- mapper
|   |-- mapper.csv
|-- local_functions.py
|-- run.py (File that runs Twiddle)
|-- twiddle.cfg
```

### User Configuration

Importing config from `twiddle.config` will import the default configuration items for each of the processes,
and will also look for a user defined configuration file on the path where the application is being run from.

All of the configuration items, including all of the default options can be found [here](twiddle/data/twiddle_defaults.cfg)

### Mapper File

A mapper file defined by the user is used to defined the input data that will be extracted from
the data repository. The mapper file is a CSV in which there are multiple columns that can be filled in
to specify the data mappings. Thw following columns must be defined in the mapper:

|    Column Name    |                Description                | Options  |
| :---------------: | :---------------------------------------: | :------: |
|      dataset      | The dataset twiddle will use mappings for | Any name |
| source_field_name |         A name of a source field          | Any name |
| source_field_type |     The data type of the source field     |          |
