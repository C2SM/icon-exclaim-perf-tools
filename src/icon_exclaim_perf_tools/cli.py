import sys
from typing import Optional, Callable
import os
import click
import json
import traceback

import sqlalchemy as sqla
import sqlalchemy.orm

from icon_exclaim_perf_tools import db, log_import
from icon_exclaim_perf_tools.db import schema as db_schema

def database_option(command: Callable):
    @click.option("--database", default="database.db", help="Database file to read and write to.")
    def _command(*args, database: str, **kwargs):
        db_instance = db.setup_db(database)
        return command(db_instance, *args, **kwargs)

    _command.__doc__ = command.__doc__
    return _command


@click.group()
def cli():
    pass

@cli.command("import_log_file")
@click.argument("log_file")
@click.option("--experiment", default=None)
@click.option("--jobid", default=None, type=int)
@database_option
def import_log_file(db: sqla.orm.Session, log_file: str, experiment: Optional[str], jobid: Optional[int]):
    """Import performance data from a log file."""
    deduced_experiment, deduced_jobid = log_import.extract_metadata_from_log_path(log_file)
    if not experiment and not deduced_experiment:
        raise click.BadArgumentUsage(
            "If the experiment can not be deduced from the log file path `--experiment` is mandatory."
        )

    try:
        log_import.import_model_run_log_from_file(db, log_file, experiment=experiment, jobid=jobid)
    except log_import.ModelRunAlreadyExists:
        click.echo("Log file has already been imported. Skipping.")


@cli.command("import_log_files")
@click.argument("log_files", nargs=-1)
@database_option
def import_log_files(db, log_files: list[str]):
    """
    Import performance data from multiple LOG_FILES.

    Can be a set of log files and/or directory containing log files.

    Examples:

    \b
    - Import all log files in the current directory.
      $ icon_exclaim_perf_tools import_log_files .
    """
    if len(log_files) == 0:
        print("No log files given.")
        return

    files = []
    for path in log_files:
        if os.path.isdir(path):
            files += os.listdir(path)
        else:
            files.append(path)

    actual_files = []
    num_skipped = 0
    for log_file in files:
        if os.path.basename(log_file).startswith("LOG."):
            actual_files.append(log_file)
        else:
            num_skipped += 1

    if num_skipped:
        print(f"Skipped {num_skipped} files as they don't start with `.LOG`.")

    for log_file in actual_files:
        try:
            log_import.import_model_run_log_from_file(db, log_file)
            print(f"Successfully imported {log_file}")
        except log_import.ModelRunAlreadyExists:
            click.echo(f"Log file `{log_file}` has already been imported. Skipping.")


@cli.command("print_all")
@database_option
def print_all(db: sqla.orm.Session):
    """Print all entries in the database."""
    from . import print_utils

    print_utils.print_all(db)

HELP_TEXT_FIELDS = "Comma seperated list of attributes to output."
HELP_TEXT_WHERE = ("Restrict the output to results that fulfill the given condition (given as an "
                   "expression). E.g., `name.startswith('fused_')`")
HELP_TEXT_GROUP_BY = "Aggregate all results grouped by the given attribute."
HELP_TEXT_ORDER_BY = ("Sort the results by the given expression, e.g. `time_total.asc()` orders "
                      "the result in ascending order of the `time_total` attribute.")
HELP_TEXT_LIMIT = "Limit the number of result rows to the given number."

@cli.command("print")
@click.argument("model")
@click.option('--fields', help=HELP_TEXT_FIELDS)
@click.option('--where', multiple=True, help=HELP_TEXT_WHERE)
@click.option('--group-by', multiple=True, help=HELP_TEXT_GROUP_BY)
@click.option('--order-by', multiple=True, help=HELP_TEXT_ORDER_BY)
@click.option('--limit', type=int, default=None, help=HELP_TEXT_LIMIT)
@click.option('--virtual-field', nargs=2, multiple=True,
              help="Display an additional column whose values are computed according to the "
                   "given expression. E.g., `time_total/60`.")
@database_option
def print_(
    db: sqla.orm.Session,
    model: str,
    fields: str,
    where: list[str],
    group_by: list[str],
    order_by: list[str],
    limit: Optional[int],
    virtual_field: list  # todo
):
    """
    Output all database entries for the given MODEL.
    """
    from . import print_utils

    if fields:
        fields = [field.strip() for field in fields.split(",")]

    print_utils.print_model(db, getattr(db_schema, model), fields, where, group_by, order_by, limit, list(virtual_field))


@cli.command("compare")
@click.argument("model")
@click.option('--jobid', multiple=True)
@click.option('--fields', help=HELP_TEXT_FIELDS)
@click.option('--where', multiple=True, help=HELP_TEXT_WHERE)
@click.option('--group-by', multiple=True, help=HELP_TEXT_GROUP_BY)
@click.option('--compare-attr', multiple=True, help="Output a comparison column for this attribute.")
@click.option('--order-by', multiple=True, help=HELP_TEXT_ORDER_BY)
@click.option('--limit', type=int, default=None, help=HELP_TEXT_LIMIT)
@database_option
def compare(
    db: sqla.orm.Session,
    model: str,
    jobid: list[str],
    fields: str,
    where: list[str],
    group_by: list[str],
    compare_attr: list[str],
    order_by: list[str],
    limit: Optional[int]
):
    """Compare all entries of the given MODEL with each other (cartesian product)."""
    from . import print_utils

    if fields:
        fields = [field.strip() for field in fields.split(",")]

    print_utils.compare(
        db,
        getattr(db_schema, model),
        fields=fields,
        jobids=jobid,
        where=where,
        group_by=group_by,
        order_by=order_by,
        compare_attrs=compare_attr,
        limit=limit
    )

@cli.command("run_experiment")
@click.argument("experiment")
@click.option('--build-types', default=None,
              help="Comma seperated list of build types.")
@click.option("--force-setup", is_flag=True, default=False,
              help="Unconditionally run setup script. By default the script is only executed when "
                   "the build folder does not exist.")
@click.option("--skip-build", is_flag=True, default=False,
              help="Skip build step and just run the experiment.")
@database_option
def run_experiment(db: sqla.orm.Session, experiment: str, build_types: Optional[str], force_setup: bool, skip_build: bool):
    """Run an EXPERIMENT for all given BUILD_TYPES."""
    from . import run_experiment

    if build_types:
        parsed_build_types = [build_type.strip() for build_type in build_types.split(",")]
    else:
        parsed_build_types = run_experiment.VALID_BUILD_TYPES

    run_experiment.run_experiment(db, experiment, parsed_build_types, force_setup=force_setup, skip_build=skip_build)


@cli.command("print_schema")
def print_schema():
    """Print all models and their attributes represented in the database."""
    models = db_schema.get_all_models()
    for model in models:
        table = sqla.inspect(model)
        print(model.__name__)
        for column in table.c:
            print("  " + column.name)
        print()


@cli.command("help")
@click.pass_context
def help(ctx):
    for command in cli.commands.values():
        if command.name == "help":
            continue
        click.echo("-"*80)
        click.echo()
        with click.Context(command, parent=ctx.parent, info_name=command.name) as ctx:
            click.echo(command.get_help(ctx=ctx))
        click.echo()


@cli.command("export_log_to_bencher")
@click.argument("log_file")
@click.option("--experiment", default=None)
@click.option("--jobid", default=None, type=int)
def export_log_to_bencher(log_file: str, experiment: Optional[str], jobid: Optional[int]):
    """Export performance data from a log file to a bencher file."""
    deduced_experiment, _ = log_import.extract_metadata_from_log_path(log_file)
    if not experiment and not deduced_experiment:
        raise click.BadArgumentUsage(
            "If the experiment can not be deduced from the log file path `--experiment` is mandatory."
        )

    try:
        model_run = log_import.import_model_run_log_from_file(db.setup_db(":memory:"), log_file, experiment=experiment, jobid=jobid)
        
        # Generate a JSON file with all the timer data in the format expected by Bencher -Bencher Metric Format- (needed for Continuous Benchmarking).
        bencher_metric_format = {model_run.experiment: {}}
        
        experiment = bencher_metric_format[model_run.experiment]
        for timer in model_run.timer:
            # assert timer.name not in experiment
            if timer.name in experiment:
                continue
            experiment[timer.name] = {
                "value": timer.time_avg,
                "lower_value": timer.time_min,
                "upper_value": timer.time_max,
            }
        
        bencher_file_name = f"bencher_{model_run.experiment}_{model_run.jobid}_{model_run.mode}.json"
        with open(bencher_file_name, "w") as f:
            json.dump(bencher_metric_format, f, indent=2)

        click.echo(bencher_file_name)

    except Exception as e:
        click.echo("An unexpected error occurred:", err=True)
        traceback.print_exc(file=sys.stderr)


if __name__ == '__main__':
    cli()
