import functools
import signal
import os
import textwrap

from icon_exclaim_perf_tools.utils.pmap import pmap
from icon_exclaim_perf_tools.utils.subproccess import execute_command
from icon_exclaim_perf_tools import log_import

import sqlalchemy as sqla
import sqlalchemy.orm


make_command = "salloc -p pp-short -N 1 -c 20 -- make -j20"
#make_command = "srun -p pp-short -N 1 -c 20 -- sleep 20"
#make_command = "salloc -p pp-short -N 1 -c 20 -- bash -c 'echo 123'"
#make_command = "sleep 1"
run_command = "end_date='2021-06-20T13:00:00Z' sbatch --wait --partition normal " # TODO: change back to debug queue

VALID_BUILD_TYPES: list[str] = [
    "build_substitution",
    "build_substitution_temporaries",
    "build_substitution_fused",
    "build_substitution_fused_temporaries",
    "build_verification",
    "build_verification_temporaries",
    "build_verification_fused",
    "build_verification_fused_temporaries",
    #"build_serialize",
    "build_acc",
    #"build_cpu",
]

def parse_line_for_pid(line):
    if "Submitted batch job" in str(line):
        split_line = line.split()
        return split_line[3]
    elif "Granted job allocation" in str(line):
        split_line = line.split()
        return split_line[4]
    else:
        return False


# TODO: fail if folders do not exists
def get_build_folder(build_type: str) -> str:
    return f"./icon-exclaim/{build_type}"

max_length: int = functools.reduce(lambda l, build_type: max(l, len(build_type)), VALID_BUILD_TYPES, 0)

def execute_setup(build_type: str, *, force: bool):
    build_folder = get_build_folder(build_type)
    build_folder_exists = os.path.exists(build_folder)
    if force or not build_folder_exists:
        if not build_folder_exists:
            print(f"Build folder {build_folder} does not exist. Executing setup script.")
        else:
            print(f"Force executing setup script.")
        return execute_command(f"salloc -p pp-short -N 1 -c 20 -- ./setup.sh {build_type}", strip=False, combine_output=True, include_output_in_error=False, cb=lambda line: print(f"[Setup {build_type:{max_length}}]: {line}"))


def execute_make(build_type: str):
    print("Executing make")
    return execute_command(
        make_command,
        cwd=get_build_folder(build_type),
        #combine_output=True,
        cb=lambda line: print(f"[Build {build_type:{max_length}}]: {line}")
    )


def execute_experiment(build_type: str, db_path, experiment: str):
    from icon_exclaim_perf_tools.db import setup_db
    db = setup_db(db_path)

    output_prefix = f"[Experiment {build_type:{max_length}}]: "

    cmd = run_command + " exp." + experiment + ".run"
    if not "verification" in build_type:
        cmd = cmd + " tool=nvprof"
    else:
        cmd = cmd

    print(output_prefix + cmd)
    jobid = None
    log_file = None
    def cb(line):
        nonlocal jobid, log_file
        print(output_prefix+line)

        if parse_line_for_pid(line):
            jobid = parse_line_for_pid(line)
            log_file = get_build_folder(build_type) + "/run/" + f"LOG.exp.{experiment}.run." + jobid + ".o"
            print(output_prefix + f"Logs will be written to: {log_file}")

    def signal_handler(sig, *args):
        if sig == signal.SIGINT:
            print(output_prefix+"Canceling job {jobid}")
            execute_command(f"scancel {jobid}")

    stdout, stderr = execute_command(
        cmd,
        cwd=get_build_folder(build_type)+"/run",
        # combine_output=True,
        cb=cb,
        signal_handler=signal_handler
    )

    print(f"Importing log file {log_file}")
    log_import.import_model_run_log_from_file(db, log_file)


def run_experiment(
    db: sqla.orm.Session,
    experiment: str,
    build_types: list[str],
    *,
    force_setup: bool,
    skip_build: bool
):
    # Setup
    pmap(functools.partial(execute_setup, force=force_setup), build_types)

    # Build
    if skip_build:
        print("Skipping build step.")
    else:
        pmap(execute_make, build_types)

    # Execute experiment
    results, errors = pmap(
        functools.partial(execute_experiment, db_path=db.bind.url.database, experiment=experiment),
        #[build_type for _ in range(10) for build_type in build_types],
        [build_type for build_type in build_types],
        ignore_errors=True
    )
    for build_mode, error in errors:
        print(f"Execution for build_mode {build_mode} failed.")
        print(textwrap.indent(str(error.ex), "  "))