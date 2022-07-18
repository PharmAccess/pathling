import click
from functools import update_wrapper
from pathling import PathlingContext
from os import path
from glob import iglob
from pyspark.sql.functions import asc
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pathling.etc import find_jar


@click.group()
def cli():
    pass

def with_pathling(func):
    def do(**kwargs):
        click.echo("Creating Pathling Context")
        conf = SparkConf() \
            .setAppName("pathing-cli") \
            .set("spark.driver.memory", "8G") \
            .set("spark.jars.packages", "io.delta:delta-core_2.12:1.2.1") \
            .set("spark.jars", find_jar())
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        pc = PathlingContext.create(spark)
        return func(pc, **kwargs)

    return update_wrapper(do, func)


@cli.command()
@click.argument('input')
@click.argument('output')
@click.option('--resource', required=False, default=None, type=str)
@click.option('--input-type', required=False, default=None, type=str)
@click.option('--write-mode', required=False, default="overwrite", type=str)
@with_pathling
def encode(pc, input, output, resource, input_type, write_mode):
    """ 
    """
    click.echo(f"Reading resources from: '{input}'")
    input_df = pc.spark.read.text(input)

    if resource is None:
        basename = path.basename(input)
        filename, extension = path.splitext(basename)
        resource = filename
        click.echo(f"Derived resource: '{resource}' from file name: '{basename}'")

    click.echo(f"Encoding resource: '{resource}' from type: '{input_type}'")
    encoded_df = pc.encode(input_df, resource, input_type)
    click.echo(f"Writing encoded resource to: '{output}' with mode: '{write_mode}'")
    encoded_df.write.parquet(output, mode=write_mode)


@cli.command()
@click.argument('input-dir')
@click.argument('output-dir')
@click.option('--include', required=False, default='*', type=str)
@click.option('--input-type', required=False, default=None, type=str)
@with_pathling
def encode_db(pc, input_dir, output_dir, include, input_type):
    """ Convert the full graph to the plain edges representation with numerical ids
        to be used by dga or as an input for further conversion to metis
    """

    for input in iglob(path.join(input_dir, include)):
        click.echo(f"Reading resources from: '{input}'")
        input_df = pc.spark.read.text(input)

        basename = path.basename(input)
        filename, extension = path.splitext(basename)
        resource = filename
        click.echo(f"Derived resource: '{resource}' from file name: '{basename}'")

        output = path.join(output_dir, resource + ".parquet")
        write_mode = "overwrite"

        click.echo(f"Encoding resource: '{resource}' from type: '{input_type}'")
        encoded_df = pc.encode(input_df, resource, input_type)
        click.echo(f"Writing encoded resource to: '{output}' with mode: '{write_mode}'")
        encoded_df.orderBy(asc("id")).write.format("delta").mode(write_mode).option(
                "overwriteSchema", "true").save(output)
