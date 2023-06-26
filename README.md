# suzuki-final-project
Final Project

## Setup

In order to run the environment, first create the virtual environment by running the following command inside of the root directory:

`python -m virtualenv venv`

Afterwards, activate the virtual environment by running:

`venv\Scripts\activate`

And then install dependencies by running:

`pip install -r requirements.txt`

## Directory Structure

The directory structure is as follows:

```
suzuki-final-project
├───data
    ├───old
│   ├───processed
│   └───raw (Empty)
├───graphs
├───notebooks
├───sheet_manipulation
├───src
│   └───data
```

## Data

The data directory contains the raw and processed data. The raw data is stored in the `raw` directory and the processed data is stored in the `processed` directory.

## Graphs

The graphs directory contains the graphs generated for the project.

## Notebooks

The notebooks directory contains the Jupyter notebooks used for the project.

## Sheet Manipulation

The sheet manipulation directory contains the scripts used for manipulating the raw shipments and invoices data into a unified table.

## Source (src)

The source directory contains the source code for the project simulation. The data directory contains the tables used for running the simulation, the simulation results analysis and the corresponding graphs.


## Running the Project

In order to run the project, first activate the virtual environment by running:

`venv\Scripts\activate`

Under the `globals.py` file, change the `PLACEMENT` value to the `placement` you'd like to test. You can also change `MAX_DATETIME` to the maximal datetime you'd like to test.

Then, run the following command to run the project:

`python src/simulation.py`

## Running the Tests

In order to run the tests, first activate the virtual environment by running:

`venv\Scripts\activate`

Then, run the following command to run the tests:

`python -m pytest tests`
