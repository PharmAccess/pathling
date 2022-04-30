Python API for Pathling
=======================

This is the Python API for [Pathling](https://pathling.csiro.au). It currently 
supports encoding of JSON bundles and resources into Apache Spark dataframes. 

## Installation

Prerequisites: 

- Python 3.8+ with pip 
- PySpark 3.1+
 
To install, run this command:
 
```bash
pip install pathling  
```
    
## Usage

The code below shows an example of using the Pathling API to encode Patient 
resources from FHIR JSON bundles:

```python
from pyspark.sql import SparkSession
from pathling.r4 import bundles
from pathling.etc import find_jar

spark = SparkSession.builder \
    .appName('pathling-test') \
    .master('local[*]') \
    .config('spark.jars', find_jar()) \
    .getOrCreate()
        
json_bundles = bundles.load_from_directory(spark, 'examples/data/bundles/R4/json/')
patients = bundles.extract_entry(spark, json_bundles, 'Patient')
patients.show()
```
    
More usage examples can be found in the `examples` directory.

## Development setup

Create an isolated python environment with `miniconda3`, e.g:

```bash
conda create -n pathling-dev python=3.8
conda activate pathling-dev
```

To configure the environment for development, run these commands:

```bash
mvn -f ../../encoders/ -DskipTests=true clean install
mvn clean compile
pip install -r ../../dev/dev-requirements.txt
pip install -e .
```
    
To run the tests and build the distribution package, run this command:

```bash
mvn clean package
```

Pathling is copyright © 2018-2022, Commonwealth Scientific and Industrial
Research Organisation
(CSIRO) ABN 41 687 119 230. Licensed under
the [CSIRO Open Source Software Licence Agreement](./LICENSE.md).
