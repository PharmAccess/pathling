Python API for Pathling
=======================

This is the Python API for [Pathling](https://pathling.csiro.au). It provides a 
set of tools that aid the use of FHIR terminology services and FHIR data within 
Python applications and data science workflows.

[View the API documentation &rarr;](https://pathling.csiro.au/docs/python/pathling.html)

## Installation

Prerequisites:

- Python 3.8+ with pip

To install, run this command:

```bash
pip install pathling  
```

## Encoders

The Python library features a set of encoders for converting FHIR data into
Spark dataframes.

### Reading in NDJSON

[NDJSON](http://ndjson.org) is a format commonly used for bulk FHIR data, and
consists of files (one per resource type) that contains one JSON resource per
line.

```python
from pathling import PathlingContext

pc = PathlingContext.create()

# Read each line from the NDJSON into a row within a Spark data set.
ndjson_dir = '/some/path/ndjson/'
json_resources = pc.spark.read.text(ndjson_dir)

# Convert the data set of strings into a structured FHIR data set.
patients = pc.encode(json_resources, 'Patient')

# Do some stuff.
patients.select('id', 'gender', 'birthDate').show()
```

### Reading in Bundles

The FHIR [Bundle](https://hl7.org/fhir/R4/bundle.html) resource can contain a
collection of FHIR resources. It is often used to represent a set of related
resources, perhaps generated as part of the same event.

```python
from pathling import PathlingContext

pc = PathlingContext.create()

# Read each Bundle into a row within a Spark data set.
bundles_dir = '/some/path/bundles/'
bundles = pc.spark.read.text(bundles_dir, wholetext=True)

# Convert the data set of strings into a structured FHIR data set.
patients = pc.encode_bundle(bundles, 'Patient')

# JSON is the default format, XML Bundles can be encoded using input type.
# patients = pc.encodeBundle(bundles, 'Patient', inputType=MimeType.FHIR_XML)

# Do some stuff.
patients.select('id', 'gender', 'birthDate').show()
```

## Terminology functions

The library also provides a set of functions for querying a FHIR terminology
server from within your queries and transformations.

### Value set membership

The `member_of` function can be used to test the membership of a code within a
[FHIR value set](https://hl7.org/fhir/valueset.html). This can be used with both
explicit value sets (i.e. those that have been pre-defined and loaded into the
terminology server) and implicit value sets (e.g. SNOMED CT
[Expression Constraint Language](http://snomed.org/ecl)).

In this example, we take a list of SNOMED CT diagnosis codes and
create a new column which shows which are viral infections. We use an ECL
expression to define viral infection as a disease with a pathological process
of "Infectious process", and a causative agent of "Virus".

```python
result = pc.member_of(csv, to_coding(csv.CODE, 'http://snomed.info/sct'),
                      to_ecl_value_set("""
<< 64572001|Disease| : (
  << 370135005|Pathological process| = << 441862004|Infectious process|,
  << 246075003|Causative agent| = << 49872002|Virus|
)
                      """), 'VIRAL_INFECTION')
result.select('CODE', 'DESCRIPTION', 'VIRAL_INFECTION').show()
```

Results in:

| CODE      | DESCRIPTION               | VIRAL_INFECTION |
|-----------|---------------------------|-----------------|
| 65363002  | Otitis media              | false           |
| 16114001  | Fracture of ankle         | false           |
| 444814009 | Viral sinusitis           | true            |
| 444814009 | Viral sinusitis           | true            |
| 43878008  | Streptococcal sore throat | false           |

### Concept translation

The `translate` function can be used to translate codes from one code system to
another using maps that are known to the terminology server. In this example, we
translate our SNOMED CT diagnosis codes into Read CTV3.

```python
result = pc.translate(csv, to_coding(csv.CODE, 'http://snomed.info/sct'),
                      'http://snomed.info/sct/900000000000207008?fhir_cm='
                      '900000000000497000',
                      output_column_name='READ_CODE')
result = result.withColumn('READ_CODE', result.READ_CODE.code)
result.select('CODE', 'DESCRIPTION', 'READ_CODE').show()
```

Results in:

| CODE      | DESCRIPTION               | READ_CODE |
|-----------|---------------------------|-----------|
| 65363002  | Otitis media              | X00ik     |
| 16114001  | Fracture of ankle         | S34..     |
| 444814009 | Viral sinusitis           | XUjp0     |
| 444814009 | Viral sinusitis           | XUjp0     |
| 43878008  | Streptococcal sore throat | A340.     |

### Subsumption testing

Subsumption test is a fancy way of saying "is this code equal or a subtype of
this other code".

For example, a code representing "ankle fracture" is subsumed
by another code representing "fracture". The "fracture" code is more general,
and using it with subsumption can help us find other codes representing
different subtypes of fracture.

The `subsumes` function allows us to perform subsumption testing on codes within
our data. The order of the left and right operands can be reversed to query
whether a code is "subsumed by" another code.

```python
# 232208008 |Ear, nose and throat disorder|
left_coding = Coding('http://snomed.info/sct', '232208008')
right_coding_column = to_coding(csv.CODE, 'http://snomed.info/sct')

result = pc.subsumes(csv, 'IS_ENT',
                     left_coding=left_coding,
                     right_coding_column=right_coding_column)

result.select('CODE', 'DESCRIPTION', 'IS_ENT').show()
```

Results in:

| CODE      | DESCRIPTION       | IS_ENT |
|-----------|-------------------|--------|
| 65363002  | Otitis media      | true   |
| 16114001  | Fracture of ankle | false  |
| 444814009 | Viral sinusitis   | true   |

### Terminology server authentication

Pathling can be configured to connect to a protected terminology server by
supplying a set of OAuth2 client credentials and a token endpoint.

Here is an example of how to authenticate to
the [NHS terminology server](https://ontology.nhs.uk/):

```python
from pathling import PathlingContext

pc = PathlingContext.create(
    terminology_server_url='https://ontology.nhs.uk/production1/fhir',
    token_endpoint='https://ontology.nhs.uk/authorisation/auth/realms/nhs-digital-terminology/protocol/openid-connect/token',
    client_id='[client ID]',
    client_secret='[client secret]'
)
```

## Installation in Databricks

To make the Pathling library available within notebooks, navigate to the
"Compute" section and click on the cluster. Click on the "Libraries" tab, and
click "Install new".

Install both the `pathling` PyPI package, and
the `au.csiro.pathling:library-api`
Maven package. Once the cluster is restarted, the libraries should be available
for import and use within all notebooks.

By default, Databricks uses Java 8 within its clusters, while Pathling requires
Java 11. To enable Java 11 support within your cluster, navigate to __Advanced
Options > Spark > Environment Variables__ and add the following:

```bash
JNAME=zulu11-ca-amd64
```

See the Databricks documentation on
[Libraries](https://docs.databricks.com/libraries/index.html) for more
information.

## Spark cluster configuration

If you are running your own Spark cluster, or using a Docker image (such as
[jupyter/pyspark-notebook](https://hub.docker.com/r/jupyter/pyspark-notebook))
,
you will need to configure Pathling as a Spark package.

You can do this by adding the following to your `spark-defaults.conf` file:

```
spark.jars.packages au.csiro.pathling:library-api:[some version]
```

See the [Configuration](https://spark.apache.org/docs/latest/configuration.html)
page of the Spark documentation for more information about `spark.jars.packages`
and other related configuration options.

To create a Pathling notebook Docker image, your `Dockerfile` might look like
this:

```dockerfile
FROM jupyter/pyspark-notebook

USER root
RUN echo "spark.jars.packages au.csiro.pathling:library-api:[some version]" >> /usr/local/spark/conf/spark-defaults.conf

USER ${NB_UID}

RUN pip install --quiet --no-cache-dir pathling && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"
```

Pathling is copyright © 2018-2022, Commonwealth Scientific and Industrial
Research Organisation
(CSIRO) ABN 41 687 119 230. Licensed under
the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
