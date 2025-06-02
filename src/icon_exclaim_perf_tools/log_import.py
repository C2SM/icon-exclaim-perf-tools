import dataclasses
import re

from icon_exclaim_perf_tools.db.schema import *

@dataclasses.dataclass
class LineCursor:
    lines: list[str]
    current_line_index = 0

    def __next__(self) -> str:
        self.current_line_index += 1
        if self.current_line_index >= len(self.lines):
            raise StopIteration
        return self.lines[self.current_line_index - 1]

    def __iter__(self) -> "LineCursor":
        return self

    def current_line(self):
        return self.lines[self.current_line_index]

    def skip(self, pattern: str, strip=True):
        line = self.current_line()
        if strip:
            line = line.strip()
        if isinstance(pattern, str):
            if line != pattern:
                raise ValueError(f"Expected line to be: \n  `{pattern}`\nbut got\n  `{line}`")
        elif isinstance(pattern, re.Pattern):
            match = pattern.search(line)
            if not match:
                raise ValueError(f"Expected line to match: \n  `{pattern}`\nbut got\n  `{line}`")

        next(self)  # skip line only after exception for easier debugging
        return line

    def revert(self):
        self.current_line_index -= 1
        return self

    def rewind(self):
        self.current_line_index = 0
        return self


class ModelRunAlreadyExists(ValueError):
    pass

def convert_to_seconds(time_str: str) -> float:
    multipliers = {'ns': 1e-9, 'us': 1e-6, 'ms': 1e-3, 's': 1, 'm': 60, 'h': 60*60}
    for unit in multipliers.keys():
        if time_str.endswith(unit):
            unitless_time = time_str[:-len(unit)]
            break
    assert re.match(r'^[\d.]+$', unitless_time)  # must be a number only
    return float(unitless_time) * multipliers[unit]

def convert_to_seconds_icon(time_str: str) -> float:
    """
    Convert time as given in icon timer report into seconds
    #>>> convert_to_seconds_icon("05m55s")
    #355.0
    >>> convert_to_seconds_icon("0.123s")
    0.123
    """
    time_in_seconds = 0
    for match in re.finditer(r"([0-9]+(\.[0-9]+){0,1}(m|s|ms|us|ns)+)", time_str):
        time_in_seconds += convert_to_seconds(match.group(1))
    return time_in_seconds

def import_nvtx_range(
    db: sqla.orm.Session,
    run: IconRun,
    nvtx_range_name: str,
    lines: list[str]
) -> None:
    line_prefix_len = len("          Range:  ")

    parsed_data = {}
    current_category = None

    for line in lines:
        if not line.strip():
            continue

        line_prefix, line = line[0:line_prefix_len].strip(), line[line_prefix_len:]
        if line_prefix in ["Range:", "GPU activities:", "API calls:"]:
            current_category = line_prefix[:-1]
            parsed_data[current_category] = []

        time_percent, time, calls, avg, min_val, max_val, name = line.split(None, 6)
        parsed_data[current_category].append({
            "name": name,
            "num_calls": int(calls),
            "time_total": convert_to_seconds(time),
            "time_avg": convert_to_seconds(avg),
            "time_min": convert_to_seconds(min_val),
            "time_max": convert_to_seconds(max_val),
        })

    assert len(parsed_data["Range"]) == 1
    nvtx_range = NVTXRange.create(
        db,
        run = run,
        name = nvtx_range_name,
        **{k: v for k, v in parsed_data["Range"][0].items() if k != "name"}
    )

    for kernel_call_data in parsed_data["GPU activities"]:
        NVTXRangeCall.create(db, type_=NVTXRangeCallType.KERNEL, nvtx_range=nvtx_range, **kernel_call_data)

    for api_call_data in parsed_data["API calls"]:
        NVTXRangeCall.create(db, type_=NVTXRangeCallType.API, nvtx_range=nvtx_range, **api_call_data)

    return parsed_data

def import_nvtx_ranges(
    db: sqla.orm.Session,
    model_run: IconRun,
    line_iterator: LineCursor
):
    range_block_pattern = re.compile(r'^==\d+==\s+Range "(.+)"')

    thread_line = next(line_iterator, None)
    assert re.compile(r'^==\d+==   Thread "<unnamed>" \(id = \d+\)$').match(thread_line)
    domain_line = next(line_iterator, None)
    assert re.compile(r'^==\d+==     Domain "<unnamed>"$').match(domain_line)

    for line in line_iterator:
        if (match := re.search(range_block_pattern, line)):
            nvtx_range_name: str = match.group(1)
            nvtx_range_lines: list[str] = []
            while (line := next(line_iterator, None)).strip() != "":  # read the entire block
                nvtx_range_lines.append(line)
            import_nvtx_range(db, model_run, nvtx_range_name, nvtx_range_lines[1:])
        else:  # if the line is not a range block we are done
            line_iterator.revert()  # revert iterator to previous line
            break

def import_timer_report(
    db: sqla.orm.Session,
    model_run: IconRun,
    line_iterator: LineCursor
) -> None:
    columns = {  # careful: order here matters
        "name": "name",
        "num_calls": "# calls",
        "time_min": "t_min",
        "_min_rang": "min rank",
        "time_avg": "t_avg",
        "time_max": "t_max",
        "_max_rang": "max rank",
        "_total_time_min": "total min (s)",  # numbers are equal to total time so skip
        "_total_time_min_rank": "total min rank",
        "time_total": "total max (s)",
        "_total_time_max_rank": "total max rank",
        "_total_time_avg": "total avg (s)",  # numbers are equal to total time so skip
        "_num_pe": "# PEs"
    }
    # skip header
    header_dash_pattern = re.compile("([-]+)".join(["(\\s+)"] * (len(columns.values())+1)))
    line_iterator.skip("")
    header_dash_line = line_iterator.current_line()
    line_iterator.skip(header_dash_pattern, strip=False)
    line_iterator.skip(re.compile("\\s+".join([re.escape(column) for column in columns.values()])))
    line_iterator.skip(header_dash_pattern, strip=False)
    line_iterator.skip("")

    # pattern that extracts the values from each column of a row
    column_pattern = re.compile("".join(
        [f"(.{{{len(el)}}})" if i % 2 else f".{{{len(el)}}}" for i, el in
         enumerate(header_dash_pattern.search(header_dash_line).groups())]))

    last_level_stack = [-1]
    last_entry_stack = [None]
    for i, line in enumerate(line_iterator):
        if re.match(r"-+", line.strip()):  # last line consists of just dashes
            break
        values = [*column_pattern.search(line).groups()]
        level = values[0].index("L") if "L" in values[0] else 0
        values[0] = values[0].replace("L", "")
        entry_data = {k: v.strip() for k, v in zip(columns.keys(), values, strict=True)}
        entry_data["time_min"] = convert_to_seconds_icon(entry_data["time_min"])
        entry_data["time_max"] = convert_to_seconds_icon(entry_data["time_max"])
        entry_data["time_avg"] = convert_to_seconds_icon(entry_data["time_avg"])
        new_entry = TimerReportEntry.create(
            db,
            run=model_run,
            **{k: v for k, v in entry_data.items() if not k.startswith("_")}
        )

        while level < last_level_stack[-1]:
            last_level_stack.pop()
            last_entry_stack.pop()

        parent = None
        if level > last_level_stack[-1]:
            parent = last_entry_stack[-1]
            last_level_stack.append(level)
            last_entry_stack.append(new_entry)
        elif level == last_level_stack[-1]:
            parent = last_entry_stack[-2]
            last_entry_stack[-1] = new_entry
        if parent:
            new_entry.parent = parent
            # new_entry.save()  # TODO: still needed?
        #db.commit()

        if i == 0:
            # This is somewhat strange in the log-file. There are other nodes with the same level
            # as `Total`, but timing wise they appear to be children. We artificially set their
            # parent to the `Total` node here.
            last_entry_stack[0] = new_entry
    pass


def import_subdomains(
    db: sqla.orm.Session,
    model_run: IconRun,
    line_iterator: LineCursor
) -> None:
    columns = {
        "icon_name": "ICON name",
        "dsl_name": "DSL name",
        "integer": "integer",
        "start_index": "start index",
        "end_index": "end index"
    }

    subdomain_line = next(line_iterator)
    line_iterator.skip("")
    column_name_line = next(line_iterator)
    header_dash_line = next(line_iterator)
    line_iterator.skip("")

    element_type = re.match(r"\[SUBDOMAINS\]:\s*(.*)", subdomain_line.strip()).group(1).strip()

    header_dash_pattern = re.compile("([-]+)".join(["(\\s+)"] * (len(columns.values())+1)))
    # pattern that extracts the values from each column of a row
    column_pattern = re.compile("".join(
        [f"(.{{{len(el)}}})" if i % 2 else f".{{{len(el)}}}" for i, el in
         enumerate(header_dash_pattern.search(header_dash_line).groups())]))

    for line in line_iterator:
        if not line.strip():  # last line consists of just dashes
            break
        values = [value.strip() for value in column_pattern.search(line).groups()]
        Subdomain.create(
            db,
            element_type=element_type,
            run=model_run,
            **dict(zip(columns.keys(), values))
        )


def extract_metadata_from_log_path(text):
    """
    Given a path to a log file try to determine the experiment and jobid.

    >>> parse_log_file_path("LOG.exp.mch_ch_r04b09_dsl.run.10134150.o")
    ('mch_ch_r04b09_dsl', '10134150')
    """
    pattern = r'LOG\.exp\.([^\.]+)\.run\.(\d+)'
    matches = re.search(pattern, text)
    if matches:
        experiment, jobid = matches.groups()
        return experiment, jobid
    else:
        return None, None


def extract_build_mode_from_executable(line: str) -> ModelRunMode:
    match = re.search(r'build_([^/\s]+)', line)
    if not match:
        return None
    run_mode: str = match.group(1)
    if run_mode == "acc":
        return ModelRunMode.OPENACC
    else:
        return ModelRunMode[run_mode.upper()]


def import_model_run_log(
    db: sqla.orm.Session,
    experiment: str,
    log_content: str,
    jobid: Optional[int] = None,
) -> IconRun:
    if jobid:
        existing_run = db.execute(sqla.select(IconRun).where(IconRun.jobid==jobid)).fetchone()
        if existing_run:
            raise ModelRunAlreadyExists(f"Model run entry with jobid {jobid} already exists.")

    model_run = IconRun(experiment=experiment, raw_log=log_content)
    if jobid:
        model_run.jobid = jobid

    nvtx_pattern = re.compile(r'^==\d+== NVTX result:')

    lines: list[str] = log_content.split('\n')

    line_iterator: LineCursor = LineCursor(lines)
    
    # Determine the build mode from the log file
    for line in line_iterator:
        if line.strip().startswith("executable:"):
            mode = extract_build_mode_from_executable(line)
            if mode is not None:
                model_run.mode = mode
        elif (match := re.search(r'\bBUILD_(GPU2PY|ACC|CPU2PY|CPU)\b', line)):
            model_run.mode = ModelRunMode[match.group(1).upper()]

    assert model_run.mode is not None, "Could not determine the build mode from the log file!"
    
    line_iterator.rewind()
    for line in line_iterator:
        if nvtx_pattern.match(line):
            import_nvtx_ranges(db, model_run, line_iterator)
        elif line.strip().startswith("Timer report,"):
            import_timer_report(db, model_run, line_iterator)
        elif line.strip().startswith("[SUBDOMAINS]"):
            import_subdomains(db, model_run, line_iterator.revert())

    db.add(model_run)
    return model_run

def import_model_run_log_from_file(
    db: sqla.orm.Session,
    log_file: str,
    *,
    experiment: Optional[str] = None,
    jobid: Optional[int] = None,
) -> IconRun:
    # read log from file
    with open(log_file, "r") as f:
        log_content = f.read()

    deduced_experiment, deduced_jobid = extract_metadata_from_log_path(log_file)
    if not experiment:
        if deduced_experiment:
            experiment = deduced_experiment
        else:
            raise ValueError("If the experiment can not be deduced from the log file path the `experiment` argument is mandatory.")

    if not jobid:
        if deduced_jobid:
            jobid = deduced_jobid

    return import_model_run_log(db, experiment, log_content, jobid=jobid)
