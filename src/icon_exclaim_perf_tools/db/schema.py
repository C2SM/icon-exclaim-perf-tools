import dataclasses
import enum
import typing
from typing import Optional

import sqlalchemy as sqla
import zlib
import sqlalchemy.orm
import sqlalchemy.sql.sqltypes

abstract_models = ["NVTXRangeCall"]

def get_all_models():
    models = []
    for var in globals().values():
        if isinstance(var, type) and issubclass(var, Model) and not var is Model:
            if var.__module__.startswith(
                    "icon_exclaim_perf_tools") and var.__name__ not in abstract_models:
                models.append(var)
    return models

def enum_field(enum_cls):
    class EnumType(sqla.types.TypeDecorator):
        impl = sqla.types.String
        cache_ok = True

        def coerce_compared_value(self, op, value):
            # required for comparison expressions with the enum like enum_field.startswith(...)
            if isinstance(value, str):
                return sqlalchemy.sql.sqltypes.String()
            else:
                return self

        def process_bind_param(self, value, dialect):
            return value.value

        def process_result_value(self, value, dialect):
            return enum_cls[value.upper()]
    return EnumType


class CompressedField(sqla.types.TypeDecorator):
    impl = sqla.types.String

    def process_bind_param(self, value, dialect):
        return zlib.compress(value.encode())

    def process_result_value(self, value, dialect):
        return zlib.decompress(value).decode()


class Model(sqla.orm.DeclarativeBase):
    # TODO: fix
    id: sqla.orm.Mapped[int] = sqla.orm.mapped_column(primary_key=True)
    #def __repr__(self):
    #    if hasattr(type(self), "REPR_FIELDS"):
    #        repr_fields = type(self).REPR_FIELDS
    #        return f"{type(self).__name__}(" + ",".join([field + "=" + repr(getattr(self, field)) for field in repr_fields]) + ")"
    #    return super().__repr__()

    @classmethod
    def create(cls, db: sqla.orm.Session, *args, **kwargs):
        obj = cls(*args, **kwargs)
        db.add(obj)
        db.commit()
        return obj

class ModelRunMode(enum.Enum):
    SUBSTITUTION = "SUBSTITUTION"
    SUBSTITUTION_TEMPORARIES = "SUBSTITUTION_TEMPORARIES"
    SUBSTITUTION_FUSED = "SUBSTITUTION_FUSED"
    SUBSTITUTION_FUSED_TEMPORARIES = "SUBSTITUTION_FUSED_TEMPORARIES"
    VERIFICATION = "VERIFICATION"
    VERIFICATION_TEMPORARIES = "VERIFICATION_TEMPORARIES"
    VERIFICATION_FUSED = "VERIFICATION_FUSED"
    VERIFICATION_FUSED_TEMPORARIES = "VERIFICATION_FUSED_TEMPORARIES"
    OPENACC = "OPENACC"

    def __str__(self):
        return self.name


class IconRun(Model):
    __tablename__ = "icon_run"
    REPR_FIELDS = ["experiment", "jobid", "mode"]
    #SKIP_PRINT_FIELDS = ["raw_log"]

    class Aggregation():
        def jobid(self):
            return sqla.case((sqla.func.count(sqla.func.distinct(self)) == 1, sqla.func.min(self)),
                             else_='NONE')

        def mode(self):
            return sqla.case((sqla.func.count(sqla.func.distinct(self)) == 1, sqla.func.min(self)),
                             else_='NONE')

    experiment: sqla.orm.Mapped[str]
    jobid: sqla.orm.Mapped[int] = sqla.orm.mapped_column(nullable=True, unique=True)
    mode = sqla.orm.mapped_column(enum_field(ModelRunMode))
    raw_log = sqla.orm.mapped_column(CompressedField)

    timer: sqla.orm.Mapped[list["TimerReportEntry"]] = sqla.orm.relationship(back_populates="run")
    nvtx_ranges: sqla.orm.Mapped[list["NVTXRange"]] = sqla.orm.relationship(back_populates="run")
    subdomains: sqla.orm.Mapped[list["Subdomain"]] = sqla.orm.relationship(back_populates="run")

    # TODO
    @property
    def test(self):
        return self.name


class Subdomain(Model):
    __tablename__ = "subdomains"

    element_type: sqla.orm.Mapped[str]
    run: sqla.orm.Mapped[IconRun] = sqla.orm.relationship(back_populates="subdomains")
    icon_name: sqla.orm.Mapped[str]
    dsl_name: sqla.orm.Mapped[str]
    integer: sqla.orm.Mapped[str]
    start_index: sqla.orm.Mapped[str]
    end_index: sqla.orm.Mapped[str]

    run_id: sqla.orm.Mapped[int] = sqla.orm.mapped_column(sqla.schema.ForeignKey(IconRun.__tablename__ + ".id"))


class TimerAggregation:
    time_min = sqla.func.min
    time_avg = sqla.func.avg
    time_max = sqla.func.max
    time_total = sqla.func.sum


class TimerReportEntry(Model):
    __tablename__ = "timer_report_entry"

    REPR_FIELDS = ["name", "run", "parent"]
    DEFAULT_COMPARE_BY = "run.jobid"
    DEFAULT_COMPARE_FIELDS = ["time_min", "time_avg", "time_max", "time_total"]
    RESTRICT_COMPARISON_BY = ["name"]

    class Aggregation(TimerAggregation):
        calls = sqla.func.sum

    id: sqla.orm.Mapped[int] = sqla.orm.mapped_column(primary_key=True)
    name: sqla.orm.Mapped[str]
    run: sqla.orm.Mapped[IconRun] = sqla.orm.relationship(back_populates="timer")
    parent: sqla.orm.Mapped[Optional["TimerReportEntry"]] = sqla.orm.relationship(back_populates="children", remote_side=[id])
    calls: sqla.orm.Mapped[int]
    time_min: sqla.orm.Mapped[float]
    time_avg: sqla.orm.Mapped[float]
    time_max: sqla.orm.Mapped[float]
    time_total: sqla.orm.Mapped[float]

    children: sqla.orm.Mapped[list["TimerReportEntry"]] = sqla.orm.relationship()

    run_id: sqla.orm.Mapped[int] = sqla.orm.mapped_column(sqla.schema.ForeignKey(IconRun.__tablename__+".id"))
    parent_id: sqla.orm.Mapped[int] = sqla.orm.mapped_column(sqla.schema.ForeignKey(__tablename__+".id"), nullable=True)
    #def __repr__(self):
    #    return f"{type(self).__name__}(name={self.name}, parent={self.parent.name if self.parent else 'None'})"


class NVTXRange(Model):
    __tablename__ = "nvtx_range"
    REPR_FIELDS = ["name", "run"]
    DEFAULT_COMPARE_BY = "run.jobid"
    DEFAULT_COMPARE_FIELDS = ["time_min", "time_avg", "time_max", "time_total"]
    RESTRICT_COMPARISON_BY = ["name"]

    class Aggregation(TimerAggregation):
        calls = sqla.func.sum

        def name(self):
            return sqla.case((sqla.func.count(sqla.func.distinct(self)) == 1, sqla.func.min(self)),
                             else_='NONE')

    name: sqla.orm.Mapped[str]

    run: sqla.orm.Mapped[Optional[IconRun]] = sqla.orm.relationship(back_populates="nvtx_ranges")

    calls: sqla.orm.Mapped[int]
    # note: these timings don't appear to be meaningful.
    time_total: sqla.orm.Mapped[float]
    time_avg: sqla.orm.Mapped[float]
    time_min: sqla.orm.Mapped[float]
    time_max: sqla.orm.Mapped[float]
    
    kernel_calls: sqla.orm.Mapped[list["NVTXRangeKernelCall"]] = sqla.orm.relationship(back_populates="nvtx_range")
    api_calls: sqla.orm.Mapped[list["NVTXRangeAPICall"]] = sqla.orm.relationship(back_populates="nvtx_range")

    run_id = sqla.orm.mapped_column(sqla.schema.ForeignKey(IconRun.__tablename__+".id"))

class NVTXRangeCall:
    DEFAULT_COMPARE_BY = "nvtx_range.run.jobid"
    RESTRICT_COMPARISON_BY = ["name", "nvtx_range.run.mode"]
    DEFAULT_COMPARE_FIELDS = ["calls", "time_min", "time_avg", "time_max", "time_total"]

    name: sqla.orm.Mapped[str]

    class Aggregation(TimerAggregation):
        calls = sqla.func.sum

    calls: sqla.orm.Mapped[int]
    time_total: sqla.orm.Mapped[float]
    time_avg: sqla.orm.Mapped[float]
    time_min: sqla.orm.Mapped[float]
    time_max: sqla.orm.Mapped[float]

class NVTXRangeKernelCall(Model, NVTXRangeCall):
    __tablename__ = "nvtx_range_kernel_call"
    REPR_FIELDS = ["name", "nvtx_range"]

    nvtx_range: sqla.orm.Mapped["NVTXRange"] = sqla.orm.relationship(back_populates="kernel_calls")
    nvtx_range_id = sqla.orm.mapped_column(sqla.schema.ForeignKey(NVTXRange.__tablename__+".id"))

class NVTXRangeAPICall(Model, NVTXRangeCall):
    __tablename__ = "nvtx_range_api_call"
    REPR_FIELDS = ["name", "nvtx_range"]

    nvtx_range: sqla.orm.Mapped["NVTXRange"] = sqla.orm.relationship(back_populates="api_calls")
    nvtx_range_id = sqla.orm.mapped_column(sqla.schema.ForeignKey(NVTXRange.__tablename__ + ".id"))