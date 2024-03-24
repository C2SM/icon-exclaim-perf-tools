import functools
import typing
from typing import Type

import sqlalchemy.orm.attributes

from icon_exclaim_perf_tools.db import schema as db_schema
from icon_exclaim_perf_tools.db.schema import *
from icon_exclaim_perf_tools import utils
import tabulate

def _seconds_to_formatted_time(seconds: float | int):
    if seconds < 1e-3:
        return f"{seconds * 1e6:.2f}us"
    elif seconds < 1:
        return f"{seconds * 1e3:.3f}ms"
    else:
        return f"{seconds:.2f}s"

def print_all(db: sqla.orm.Session):
    models = db_schema.get_all_models()
    for model in models:
        print(model.__name__)
        print_model(db, model)


def _get_all_attributes(cls: type):
    attrs = {}
    for cls_ in cls.__mro__:
        attrs = {**{k: v for k, v in vars(cls_).items() if not k.startswith("_")}, **attrs}
    return attrs


def compare(
    db: sqla.orm.Session,
    model: Type[Model],
    jobids: list[str],
    compare_attrs: Optional[list[str]] = None,
    order_by: Optional[list[str]] = None,
    where: Optional[list[str]] = None,
) -> None:
    compare_by = tuple(model.DEFAULT_COMPARE_BY.split("."))

    # alias the model in order to query data from both
    m1, m2 = sqla.orm.aliased(model), sqla.orm.aliased(model)

    # grab metadata of the two models
    m1_attrs, _, joins1, _ = utils.query.get_all_model_attrs(m1)
    m2_attrs, _, joins2, _ = utils.query.get_all_model_attrs(m2)

    # derive sql expressions for the fields we want to compare
    comparison_exprs = {}
    if compare_attrs:
        for attr_path_str in compare_attrs:
            attr_path = tuple(attr_path_str.split("."))
            attr_m1 = m1_attrs[attr_path]
            attr_m2 = m2_attrs[attr_path]
            comparison_exprs[attr_path] = sqla.func.round(((attr_m1 / attr_m2)-1)*100)
    else:
        for (attr_path1, field1), (attr_path2, field2) in zip(m1_attrs.items(), m2_attrs.items()):
            if len(attr_path1) > 1:
                continue
            assert attr_path1 == attr_path2 and field1.type == field2.type
            if isinstance(field1.type, (sqla.types.Integer, sqla.types.Numeric)):
                if not hasattr(model, "DEFAULT_COMPARE_FIELDS") or ".".join(attr_path1) in model.DEFAULT_COMPARE_FIELDS:
                    comparison_exprs[attr_path1] = sqla.func.round(((field1/field2)-1)*100)

    expr_context = utils.query.build_expression_context({
        **{("m1", *k): v for k, v in m1_attrs.items()},
        **{("m2", *k): v for k, v in m2_attrs.items()},
        **comparison_exprs
    })

    #
    # build query
    #
    # what to select
    selected_entities = [m1, m2]
    selected_entities.append(sqla.orm.Bundle("comparison_results", *comparison_exprs.values()))
    #  prefetching: also fetch the sub-models
    for joins in [joins1, joins2]:
        for (left, right, on_clause, _) in joins:
            selected_entities.append(right)

    query = sqla.select(*selected_entities)

    # join all submodels so that prefetching actually works
    for joins in [joins1, joins2]:
        for (left, right, on_clause, is_optional) in joins:
            query = query.join_from(left, right, on_clause, isouter=is_optional)

    # select every combination
    #  we don't want to compare with itself
    query = query.where(m1_attrs[compare_by] != m2_attrs[compare_by])

    query = query.where(m1_attrs[compare_by].in_(jobids))
    query = query.where(m2_attrs[compare_by].in_(jobids))

    if where:
        for where_expr in where:
            selector = eval(where_expr, expr_context.__dict__)
            query = query.where(selector)

    if order_by:
        order_by_exprs = [eval(order_by_expr, expr_context.__dict__) for order_by_expr in order_by]
        query = query.order_by(*order_by_exprs)

    # frequently comparing values is only when some attributes are equal, e.g. it doesn't
    # make sence to compare runs for different experiments.
    restrict_comparison_by = getattr(model, "RESTRICT_COMPARISON_BY", [])
    for restricted_field_path_str in restrict_comparison_by:
        restricted_field_path = tuple(restricted_field_path_str.split("."))
        query = query.where(m1_attrs[restricted_field_path] == m2_attrs[restricted_field_path])

    #print(str(query))
    #from sqlalchemy.dialects import sqlite
    #print(query.compile(dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}))
    results = [*db.execute(query)]

    # find out if the two fields are equal for all results then we only need to print them once
    equal_fields = [True] * len(m1_attrs.keys())
    for result_row in results:
        result1, result2 = result_row[0], result_row[1]
        for i, attr_path in enumerate(m1_attrs.keys()):
            value1 = functools.reduce(getattr, attr_path, result1)
            value2 = functools.reduce(getattr, attr_path, result2)
            equal_fields[i] &= value1==value2

    #
    # print results
    #
    table = []
    comparison_result_indices = {expr: i for i, expr in enumerate(comparison_exprs)}

    def handle_model_column(i: int, attr_path: tuple[str, ...]):
        compare: bool = attr_path in comparison_exprs
        get_value: typing.Callable = functools.partial(functools.reduce, getattr, attr_path)

        repr_fields = getattr(m1_attrs[attr_path].class_, "REPR_FIELDS", None)

        if not repr_fields or attr_path[-1] in repr_fields or compare:
            if equal_fields[i]:
                yield ".".join(attr_path), lambda result: get_value(result[0])
            else:
                yield ".".join(("m1", *attr_path)), lambda result: get_value(result[0])
                yield ".".join(("m2", *attr_path)), lambda result: get_value(result[1])
        if compare:
            idx = comparison_result_indices[attr_path]
            yield ".".join(attr_path), lambda result: result.comparison_results[idx]

    columns: list[tuple[str, typing.Callable]] = []
    for i, attr_path in enumerate(m1_attrs.keys()):
        columns.extend(handle_model_column(i, attr_path))
    column_names, column_accessors = zip(*columns)

    for result in results:
        row = []
        for column_accessor in column_accessors:
            row.append(column_accessor(result))
        table.append(row)

    print(tabulate.tabulate(table, headers=column_names))
    print()


def _attr_path_starts_with(attr_path: tuple[str, ...], sub_path: tuple[str, ...]):
    """
    >>> _attr_path_starts_with(("model", "submodel", "field"), ("model", "submodel"))
    True
    >>> _attr_path_starts_with(("model", "submodel"), ("other_model",))
    False
    """
    assert isinstance(attr_path, tuple) and isinstance(sub_path, tuple)
    return sub_path == attr_path[0:len(sub_path)]


def print_model(
    db: sqla.orm.Session,
    model: Type[Model],
    fields: Optional[list[str]] = None,
    where: Optional[list[str]] = None,
    group_by: Optional[list[str]] = None,
    order_by: Optional[list[str]] = None,
    limit: Optional[int] = None,
    virtual_fields: Optional[list[tuple[str, str]]] = None,
    max_field_length: int = 50
):
    mapped_attrs, virtual_attrs, joins, submodels = utils.query.get_all_model_attrs(model)

    expr_context = utils.query.build_expression_context(mapped_attrs)

    group_by_attrs = {}
    if group_by:
        group_by_columns = []
        for group_by_expr in group_by:
            expr = eval(group_by_expr, expr_context.__dict__)
            group_by_columns.append(expr)

        for attr_path, submodel in {(): model, **submodels}.items():
            # todo: test for alias
            if hasattr(submodel, "Aggregation"):
                for name, value in _get_all_attributes(submodel.Aggregation).items():
                    aggregation_function = getattr(submodel.Aggregation, name)
                    group_by_attrs[(*attr_path, name)] = aggregation_function(getattr(submodel, name))

        order_by_expr_context = utils.query.build_expression_context(group_by_attrs)
        output_attrs = group_by_attrs
    else:
        order_by_expr_context = expr_context
        output_attrs = mapped_attrs

    if fields:
        new_fields = []
        for field in fields:
            for attr_path in output_attrs.keys():
                if _attr_path_starts_with(attr_path, tuple(field.split("."))):
                    new_fields.append(attr_path)
        fields = new_fields
    else:
        fields = output_attrs.keys()

    #
    # build query
    #
    # what so select
    selected_entities = [model]
    if group_by:
        selected_entities.append(sqla.orm.Bundle("group_by_attrs", *group_by_attrs.values()))
    #  prefetching: also fetch the sub-models
    for (left, right, on_clause, _) in joins:
        selected_entities.append(right)

    query = sqla.select(*selected_entities)

    # join all submodels so that prefetching actually works
    for (left, right, on_clause, is_optional) in joins:
        # TODO: the outer join is very expensive. Use select.union for all different combinations
        #  of is_optional instead
        query = query.join_from(left, right, on_clause, isouter=is_optional)

    if where:
        for where_expr in where:
            selector = eval(where_expr, expr_context.__dict__)
            query = query.where(selector)

    if order_by:
        order_by_exprs = [eval(order_by_expr, order_by_expr_context.__dict__) for order_by_expr in order_by]
        query = query.order_by(*order_by_exprs)

    if group_by:
        query = query.group_by(*group_by_columns)

    if limit:
        query = query.limit(limit)

    # fetch all results
    import pandas
    df = pandas.read_sql(query, db.bind)
    results = [*db.execute(query)]

    #
    # print results
    #
    headers = [".".join(attr) for attr in output_attrs.keys() if attr in fields]
    table = []
    for result in results:
        row = []
        if group_by:
            result_iterator = ((attr_path, v) for attr_path, v in zip(group_by_attrs.keys(), result.group_by_attrs) if attr_path in fields)
        else:
            result = result[0]
            result_iterator = ((attr_path, functools.reduce(getattr, attr_path, result)) for attr_path in mapped_attrs.keys() if attr_path in fields)
        for attr_path, value in result_iterator:
            value = str(value)
            if len(value) > 50:
                value = value[0:50] + "..."
            row.append(value)
        table.append(row)

    print(tabulate.tabulate(table, headers=headers))
    print()