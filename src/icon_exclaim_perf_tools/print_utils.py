import ast
import dataclasses
import functools
import types
import typing
from types import SimpleNamespace
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


class AttributeAccessRecorder:
    _recorded_accesses: set[str]
    _wrapped_value: typing.Any

    def __init__(self, wrapped_value):
        self._recorded_accesses = set()
        self._wrapped_value = set()

    def __getattribute__(self, item):
        self._recorded_accesses.add(item)
        return getattr(self._value, item)


def compare(
    db: sqla.orm.Session,
    model: Type[Model],
    jobids: list[str],
    compare_attrs: Optional[list[str]] = None,
    order_by: Optional[list[str]] = None,
    where: Optional[list[str]] = None,
    group_by: Optional[list[str]] = None,
    limit: Optional[int] = None,
    fields: Optional[list[str]] = None,
) -> None:
    compare_by = tuple(model.DEFAULT_COMPARE_BY.split("."))

    # split where-expression by what models they use. if an expression only uses a single model
    # we restrict the subquery, which allows to filter before we aggregate / group-by. if both
    # models are used we only restrict the result.
    where_m1, where_m2, where_both = [], [], []
    for where_expr in (where or []):
        where_ast = ast.parse(where_expr)
        # this doesn't support shadowing of symbols, but we never expect this to happen
        used_models = {
            node.id
            for node in ast.walk(where_ast)
            if isinstance(node, ast.Name) and node.id in ['m1', 'm2']
        }
        if "m1" in used_models and "m2" not in used_models:
            where_m1.append(where_expr)
        elif "m2" in used_models and "m1" not in used_models:
            where_m2.append(where_expr)
        else:
            where_both.append(where_expr)

    # build sub-queries for the compared models
    m1_entities, m1_attrs, m1_accessors = utils.query.build_query(
        model,
        where=where_m1,
        group_by=["m1."+path_str for path_str in group_by] if group_by else None,
        as_subquery_entities="m1"
    )
    m2_entities, m2_attrs, m2_accessors = utils.query.build_query(
        model,
        where=where_m2,
        group_by=["m2." + path_str for path_str in group_by] if group_by else None,
        as_subquery_entities="m2"
    )

    # derive sql expressions for the fields we want to compare
    comparison_exprs = {}
    if compare_attrs:
        for attr_path_str in compare_attrs:
            attr_path = tuple(attr_path_str.split("."))
            attr_m1 = m1_attrs[("m1", *attr_path)]
            attr_m2 = m2_attrs[("m2", *attr_path)]
            comparison_exprs[attr_path] = sqla.func.round(((attr_m1 / attr_m2) - 1) * 100).label(
                "comparison." + ".".join(attr_path))
    else:
        for ((_, *attr_path1), field1), ((_, *attr_path2), field2) in zip(m1_attrs.items(), m2_attrs.items()):
            if len(attr_path1) > 1:
                continue
            assert attr_path1 == attr_path2 and field1.type == field2.type
            if isinstance(field1.type, (sqla.types.Integer, sqla.types.Numeric)):
                if not hasattr(model, "DEFAULT_COMPARE_FIELDS") or ".".join(attr_path1) in model.DEFAULT_COMPARE_FIELDS:
                    comparison_exprs[tuple(attr_path1)] = sqla.func.round(
                        ((field1 / field2) - 1) * 100).label("comparison." + ".".join(attr_path1))

    expr_context = utils.query.build_expression_context({
        **m1_attrs,
        **m2_attrs,
        **comparison_exprs
    })

    query = sqla.select(*m1_entities, *m2_entities, *comparison_exprs.values())

    # select every combination
    #  we don't want to compare with itself
    query = query.where(m1_attrs[("m1", *compare_by)] != m2_attrs[("m2", *compare_by)])
    # the int cast is only needed when using group-by for unclear reasons
    query = query.where(m1_attrs[("m1", *compare_by)].in_([int(jobid) for jobid in jobids]))
    query = query.where(m2_attrs[("m2", *compare_by)].in_([int(jobid) for jobid in jobids]))

    # frequently comparing values is only meaningful when some attributes are equal, e.g. it
    # doesn't make sense to compare runs for different experiments.
    restrict_comparison_by = getattr(model, "RESTRICT_COMPARISON_BY", [])
    for restricted_field_path_str in restrict_comparison_by:
        restricted_field_path = tuple(restricted_field_path_str.split("."))
        query = query.where(m1_attrs[("m1", *restricted_field_path)] == m2_attrs[("m2", *restricted_field_path)])


    for where_expr in where_both:
        selector = eval(where_expr, expr_context.__dict__)
        query = query.where(selector)

    if order_by:
        order_by_exprs = [eval(order_by_expr, expr_context.__dict__) for order_by_expr in order_by]
        query = query.order_by(*order_by_exprs)

    if limit:
        query = query.limit(limit)

    results = [*db.execute(query)]

    import pandas
    df = pandas.read_sql(query, db.bind)

    # find out if the two fields are equal for all results then we only need to print them once
    equal_fields: dict = {}
    for m1_attr_path in m1_accessors.keys():
        equal_fields.setdefault(tuple(m1_attr_path[1:]), True)
    for result in results:
        for i, ((m1_attr_path, m1_accessor), (m2_attr_path, m2_accessor)) in enumerate(zip(m1_accessors.items(), m2_accessors.items(), strict=True)):
            attr_path = tuple(m1_attr_path[1:])
            value1 = m1_accessor(result)
            value2 = m2_accessor(result)

            equal_fields[attr_path] &= value1 == value2

    # filter output columns
    output_columns = [tuple(attr_path[1:]) for attr_path in m1_attrs.keys()]
    if fields:
        new_output_columns = []
        for field in fields:
            for attr_path in output_columns:
                if _attr_path_starts_with(attr_path, tuple(field.split("."))):
                    new_output_columns.append(attr_path)
        output_columns = new_output_columns

    headers = []
    for m1_attr_path, m2_attr_path in zip(m1_accessors.keys(), m2_accessors.keys(), strict=True):
        attr_path = tuple(m1_attr_path[1:])
        if attr_path not in output_columns:
            continue
        if not equal_fields[attr_path]:
            headers.append(".".join(m1_attr_path))
            headers.append(".".join(m2_attr_path))
        else:
            headers.append(".".join(m1_attr_path[1:]))
        if attr_path in comparison_exprs:
            headers.append("Δᵣ"+".".join(m1_attr_path[1:]))

    table = []
    for result in results:
        row = []
        for (m1_attr_path, m1_accessor), (m2_attr_path, m2_accessor) in zip(m1_accessors.items(), m2_accessors.items(), strict=True):
            assert m1_attr_path[1:] == m2_attr_path[1:]
            attr_path = tuple(m1_attr_path[1:])
            if attr_path not in output_columns:
                continue
            row.append(m1_accessor(result))
            if not equal_fields[attr_path]:
                row.append(m2_accessor(result))
            if tuple(attr_path) in comparison_exprs:
                row.append(getattr(result, "comparison." + ".".join(attr_path)))
        table.append(row)
    print(tabulate.tabulate(table, headers=headers))  # , headers=column_names
    print()
    return



    # alias the model in order to query data from both
    m1, m2 = sqla.orm.aliased(model, alias=m1_subquery), sqla.orm.aliased(model, alias=m2_subquery)

    # grab metadata of the two models
    m1_attrs, _, joins1, m1_submodels = utils.query.get_all_model_attrs(m1)
    m2_attrs, _, joins2, m2_submodels = utils.query.get_all_model_attrs(m2)

    expr_context = utils.query.build_expression_context({
        **{("m1", *k): v for k, v in m1_attrs.items()},
        **{("m2", *k): v for k, v in m2_attrs.items()},
    })

    # group by
    group_by = ["nvtx_range.name", "nvtx_range.run.jobid"]
    #group_by = []
    group_by_attrs = {}
    group_by_column_exprs = []
    if group_by:
        output_columns = []
        for model_name, model, attrs, submodels in zip(["m1", "m2"], [m1, m2], (m1_attrs, m2_attrs), (m1_submodels, m2_submodels)):
            for group_by_expr in group_by:
                expr = eval(group_by_expr, getattr(expr_context, model_name).__dict__)
                group_by_column_exprs.append(expr)

            for model_path, submodel in {(): model, **submodels}.items():
                # todo: test for alias
                if hasattr(submodel, "Aggregation"):
                    for name, value in utils.query._get_all_attributes(submodel.Aggregation).items():
                        aggregation_function = getattr(submodel.Aggregation, name)
                        attr_path = (model_name, *model_path, name)
                        group_by_attrs[attr_path] = aggregation_function(
                            getattr(submodel, name)).label('.'.join(attr_path))

                        if (*model_path, name) not in output_columns: # TODO: this is ugly
                            output_columns.append((*model_path, name))

        #order_by_expr_context = utils.query.build_expression_context(group_by_attrs)
        #output_attrs_m1, output_attrs_m2 = group_by_attrs
    else:
        output_columns = [*m1_attrs.keys()]
        #order_by_expr_context = expr_context
        #output_attrs = SimpleNamespace(m1=m1_attrs, m2=m2_attrs)

    # filter outputs columns
    if fields:
        new_output_columns = []
        for field in fields:
            for attr_path in output_columns:
                if _attr_path_starts_with(attr_path, tuple(field.split("."))):
                    new_output_columns.append(attr_path)
        output_columns = new_output_columns

    # derive sql expressions for the fields we want to compare
    comparison_exprs = {}
    if compare_attrs:
        for attr_path_str in compare_attrs:
            attr_path = tuple(attr_path_str.split("."))
            attr_m1 = group_by_attrs[("m1", *attr_path)] if group_by else m1_attrs[attr_path]  # TODO: ugly
            attr_m2 = group_by_attrs[("m2", *attr_path)] if group_by else m2_attrs[attr_path]
            comparison_exprs[attr_path] = sqla.func.round(((attr_m1 / attr_m2)-1)*100).label("comparison."+".".join(attr_path))
    else:
        for (attr_path1, field1), (attr_path2, field2) in zip(m1_attrs.items(), m2_attrs.items()):
            if len(attr_path1) > 1:
                continue
            assert attr_path1 == attr_path2 and field1.type == field2.type
            if isinstance(field1.type, (sqla.types.Integer, sqla.types.Numeric)):
                if not hasattr(model, "DEFAULT_COMPARE_FIELDS") or ".".join(attr_path1) in model.DEFAULT_COMPARE_FIELDS:
                    if group_by:  # TODO: ugly
                        field1 = group_by_attrs[("m1", *attr_path1)]
                        field2 = group_by_attrs[("m2", *attr_path1)]

                    comparison_exprs[attr_path1] = sqla.func.round(((field1/field2)-1)*100).label("comparison."+".".join(attr_path1))

    expr_context = utils.query.build_expression_context({
        **{("m1", *k): v for k, v in m1_attrs.items()},
        **{("m2", *k): v for k, v in m2_attrs.items()},
        **comparison_exprs
    })

    #
    # build query
    #
    # what to select
    selected_entities = [m1, m2]  # sqla.func.sum(m1_attrs[("time_total",)]).label("agg_time_total")
    if not group_by:
        selected_entities = [*selected_entities, *comparison_exprs.values()]
    #if group_by:
    #    selected_entities = [*selected_entities, *group_by_attrs.values()]
    #selected_entities =[*selected_entities, *comparison_exprs.values()]
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

    subquery = query.subquery()

    m1_s = sqla.orm.aliased(m1, alias=subquery)
    m2_s = sqla.orm.aliased(m2, alias=subquery)
    new_query = sqla.select(m1_s, m2_s)

    bla = [*db.execute(new_query)]

    if group_by:
        query = query.group_by(*group_by_column_exprs)

    if order_by:
        order_by_exprs = [eval(order_by_expr, expr_context.__dict__) for order_by_expr in order_by]
        query = query.order_by(*order_by_exprs)

    # frequently comparing values is only meaningful when some attributes are equal, e.g. it
    # doesn't make sense to compare runs for different experiments.
    restrict_comparison_by = getattr(model, "RESTRICT_COMPARISON_BY", [])
    for restricted_field_path_str in restrict_comparison_by:
        restricted_field_path = tuple(restricted_field_path_str.split("."))
        query = query.where(m1_attrs[restricted_field_path] == m2_attrs[restricted_field_path])

    #print(str(query))
    print(query.compile(dialect=sqlite.dialect(), compile_kwargs={"literal_binds": True}))
    results = [*db.execute(query)]

    # function to get the value for a given path from a row
    def get_value_from_row(result, attr_path: tuple[str, ...]):
        if group_by:
            return getattr(result, ".".join(attr_path))
        else:
            assert attr_path[0] in ["m1", "m2"]
            model_result = result[0] if attr_path[0] == "m1" else result[1]
            return functools.reduce(getattr, attr_path[1:], model_result)

    # find out if the two fields are equal for all results then we only need to print them once
    equal_fields = [False] * len(output_columns)
    for result_row in results:
        for i, attr_path in enumerate(output_columns):
            value1 = get_value_from_row(result_row, ("m1", *attr_path))
            value2 = get_value_from_row(result_row, ("m2", *attr_path))
            equal_fields[i] &= value1==value2

    #
    # print results
    #
    table = []

    def handle_model_column(i: int, attr_path: tuple[str, ...]):
        compare: bool = attr_path in comparison_exprs
        if attr_path not in m1_attrs:  # TODO: nasty way of handling when column is part of a submodel that is not represented
            return

        repr_fields = getattr(m1_attrs[attr_path].class_, "REPR_FIELDS", None)

        if not repr_fields or attr_path[-1] in repr_fields or compare:
            if equal_fields[i]:
                yield ".".join(attr_path), lambda result: get_value_from_row(result, ("m1", *attr_path))
            else:
                yield ".".join(("m1", *attr_path)), lambda result: get_value_from_row(result, ("m1", *attr_path))
                yield ".".join(("m2", *attr_path)), lambda result: get_value_from_row(result, ("m2", *attr_path))
        if compare:
            yield ".".join(attr_path), lambda result: getattr(result, "comparison."+".".join(attr_path))

    columns: list[tuple[str, typing.Callable]] = []
    for i, attr_path in enumerate(output_columns):
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
    assert isinstance(attr_path, (tuple, list)) and isinstance(sub_path, (tuple, list))
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
    assert not virtual_fields

    query, output_attrs, accessors = utils.query.build_query(
        model,
        fields=fields,
        where=where,
        group_by=group_by,
        order_by=order_by,
        limit=limit,
    )

    # fetch all results
    #import pandas
    #df = pandas.read_sql(query, db.bind)
    results = [*db.execute(query)]

    #
    # print results
    #
    headers = [".".join(attr) for attr in output_attrs.keys()]
    table = []
    for result in results:
        row = []
        result_iterator = ((attr_path, accessors[attr_path](result)) for attr_path in output_attrs.keys())
        for attr_path, value in result_iterator:
            value = str(value)
            if len(value) > max_field_length:
                value = value[0:max_field_length] + "..."
            row.append(value)
        table.append(row)

    print(tabulate.tabulate(table, headers=headers))
    print()