# ICON Exclaim Performance tools

This repository contains tools to measure and analyse performance of icon-exclaim (right now blue-line only).


```commandline
--------------------------------------------------------------------------------

Usage: icon_exclaim_perf_tools import_log_file [OPTIONS] LOG_FILE

  Import performance data from a log file.

Options:
  --experiment TEXT
  --jobid INTEGER
  --database TEXT    Database file to read and write to.
  --help             Show this message and exit.

--------------------------------------------------------------------------------

Usage: icon_exclaim_perf_tools import_log_files [OPTIONS] [LOG_FILES]...

  Import performance data from multiple LOG_FILES.

  Can be a set of log files and/or directory containing log files.

Options:
  --database TEXT  Database file to read and write to.
  --help           Show this message and exit.

--------------------------------------------------------------------------------

Usage: icon_exclaim_perf_tools print_all [OPTIONS]

  Print all entries in the database.

Options:
  --database TEXT  Database file to read and write to.
  --help           Show this message and exit.

--------------------------------------------------------------------------------

Usage: icon_exclaim_perf_tools print [OPTIONS] MODEL

  Output all database entries for the given MODEL.

Options:
  --fields TEXT            The attributes to output.
  --where TEXT             Restrict the output to results that fulfill the
                           given condition (given as an expression). E.g.,
                           `name.startswith('fused_')`
  --group-by TEXT          Aggregate all results into groups where the given
                           attribute is equal.
  --order-by TEXT          Sort the results by the given expression, e.g.
                           `time_total.asc()` orders the result in ascending
                           order of the `time_total` attribute.
  --limit INTEGER          Limit the number of result rows to the given
                           number.
  --virtual-field TEXT...  Display an additional column whose values are
                           computed according to the given expression. E.g.,
                           `time_total/60`.
  --database TEXT          Database file to read and write to.
  --help                   Show this message and exit.

--------------------------------------------------------------------------------

Usage: icon_exclaim_perf_tools compare [OPTIONS] MODEL

  Compare all entries of the given MODEL with each other (cartesian product).

Options:
  --jobid TEXT
  --fields TEXT
  --where TEXT         Restrict the output to results that fulfill the given
                       condition (given as an expression). E.g.,
                       `name.startswith('fused_')`
  --group-by TEXT      Aggregate all results into groups where the given
                       attribute is equal.
  --compare-attr TEXT  Output a comparison column for this attribute.
  --order-by TEXT      Sort the results by the given expression, e.g.
                       `time_total.asc()` orders the result in ascending order
                       of the `time_total` attribute.
  --limit INTEGER      Limit the number of result rows to the given number.
  --database TEXT      Database file to read and write to.
  --help               Show this message and exit.

--------------------------------------------------------------------------------

Usage: icon_exclaim_perf_tools run_experiment [OPTIONS] EXPERIMENT

  Run an EXPERIMENT for all given BUILD_TYPES.

Options:
  --build-types TEXT  Comma seperated list of build types.
  --force-setup       Unconditionally run setup script. By default the script
                      is only executed when the build folder does not exist.
  --skip-build        Skip build step and just run the experiment.
  --database TEXT     Database file to read and write to.
  --help              Show this message and exit.
```