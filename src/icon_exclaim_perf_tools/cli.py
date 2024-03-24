import sys
from typing import Optional, Callable
import os
import click

import sqlalchemy as sqla
import sqlalchemy.orm

from icon_exclaim_perf_tools import db, log_import
from icon_exclaim_perf_tools.db import schema as db_schema

def database_option(command: Callable):
    @click.option("--database", default="database.db")
    def _command(*args, database: str, **kwargs):
        db_instance = db.setup_db(database)
        return command(db_instance, *args, **kwargs)

    _command.__doc__ == command.__doc__
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
    """Import performance data from multiple log files"""
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

    for log_file in files:
        try:
            log_import.import_model_run_log_from_file(db, log_file)
            print(f"Successfully imported {log_file}")
        except log_import.ModelRunAlreadyExists:
            click.echo(f"Log file `{log_file}` has already been imported. Skipping.")


@cli.command("print_all")
@database_option
def print_all(db: sqla.orm.Session):
    from . import print_utils

    print_utils.print_all(db)


@cli.command("print")
@click.argument("model")
@click.option('--fields')
@click.option('--where', multiple=True)
@click.option('--group-by', multiple=True)
@click.option('--order-by', multiple=True)
@click.option('--limit', type=int)
@click.option('--virtual-field', nargs=2, multiple=True)
@database_option
def print_(
        db: sqla.orm.Session,
        model: str,
        fields: str,
        where: list[str],
        group_by: list[str],
        order_by: list[str],
        limit: int,
        virtual_field: list  # todo
):
    from . import print_utils

    if fields:
        fields = [field.strip() for field in fields.split(",")]

    print_utils.print_model(db, getattr(db_schema, model), fields, where, group_by, order_by, limit, list(virtual_field))


@cli.command("compare")
@click.argument("model")
@click.option('--jobid', multiple=True)
@click.option('--where', multiple=True)
@click.option('--compare-attr', multiple=True)
@click.option('--order-by', multiple=True)
@database_option
def compare(db: sqla.orm.Session, model: str, jobid: list[str], where: list[str], compare_attr: list[str], order_by: list[str]):
    from . import print_utils

    print_utils.compare(db, getattr(db_schema, model), jobids=jobid, where=where, order_by=order_by, compare_attrs=compare_attr)

@cli.command("run_experiment")
@click.argument("experiment")
@database_option
def run_experiment(db: sqla.orm.Session, experiment: str):
    from . import run_experiment
    run_experiment.run_experiment(db, experiment)

if __name__ == '__main__':
    cli()