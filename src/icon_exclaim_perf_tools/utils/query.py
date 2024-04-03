import builtins
import functools
import inspect
import operator
from types import SimpleNamespace
import typing
from typing import Optional

import sqlalchemy as sqla
import sqlalchemy.orm.util
import sqlalchemy.sql.elements

from icon_exclaim_perf_tools import db
from icon_exclaim_perf_tools.db.schema import Model

ModelOrAlias = db.schema.Model | sqla.orm.util.AliasedClass[db.schema.Model]


def _get_cls_attrs(cls):
    attrs = {}
    for name in dir(cls):
        value = getattr(cls, name)
        if not name.startswith("_") and not inspect.ismethod(value):
            attrs[name] = value

    # some attrs are only declared as an annotation and then defined by the base class
    #  in order to keep the ordering sort by appearance in the annotation dict
    annotations_ = {attr: (i, j) for i, cls_ in enumerate(reversed(cls.__mro__)) for j, attr in enumerate(inspect.get_annotations(cls_).keys())}
    def comparision(value: tuple[str, typing.Any]):
        return annotations_.get(value[0], (len(cls.__mro__), 0))
    return dict(sorted(attrs.items(), key=comparision))


def _sort_dict_by_order_in_other_dict(to_be_sorted: dict, reference: dict):
    reference_order: dict[typing.Any, int] = {k: i for i, k in enumerate(reference.keys())}
    def comparision(value: tuple[typing.Any, typing.Any]):
        return reference_order[value[0]]
    return dict(sorted(to_be_sorted.items(), key=comparision))


def _attr_path_starts_with(attr_path: tuple[str, ...], sub_path: tuple[str, ...]):
    """
    >>> _attr_path_starts_with(("model", "submodel", "field"), ("model", "submodel"))
    True
    >>> _attr_path_starts_with(("model", "submodel"), ("other_model",))
    False
    """
    assert isinstance(attr_path, (tuple, list)) and isinstance(sub_path, (tuple, list))
    return sub_path == attr_path[0:len(sub_path)]


def _is_model(arg: typing.Any):
    if isinstance(arg, sqla.orm.util.AliasedClass):
        arg = sqla.inspect(arg).class_
    return inspect.isclass(arg) and issubclass(arg, db.schema.Model)


def get_all_model_attrs(model: type[db.schema.Model], prefix: tuple[str, ...] = (), subquery=None):
    """
    Given a model return all its attributes and attributes of its submodules.
    """
    ignored_models = set()
    mapped_attrs: dict = {}
    virtual_attrs: dict = {}

    joins: list[tuple[ModelOrAlias, ModelOrAlias, sqla.sql.elements.OperatorExpression, bool]] = []
    submodels: dict[tuple[str, ...], ModelOrAlias] = {}

    def _impl(
        model_or_alias: type[ModelOrAlias],
        model_path: tuple[str, ...]
    ):
        if isinstance(model_or_alias, sqla.orm.util.AliasedClass):
            model = sqla.inspect(model_or_alias).class_
        else:
            model = model_or_alias
        assert issubclass(model, db.schema.Model)

        if model in ignored_models:
            return  # TODO(tehrengruber): print at least the id for self-referential models
        ignored_models.add(model)

        for name in _get_cls_attrs(model).keys():  # TODO(tehrengruber): check vars or __dict__ since different if inheriting
            value = getattr(model_or_alias, name)

            if name.startswith("_") or name[0].isupper():
                continue
            if isinstance(value, sqla.orm.attributes.QueryableAttribute):
                if isinstance(value.property, sqla.orm.relationships.Relationship):
                    if value.property.direction == sqla.orm.relationships.RelationshipDirection.MANYTOONE:
                        submodel = value.property.entity.class_
                        submodel_alias = sqla.orm.aliased(submodel, subquery)
                        submodels[(*model_path, name)] = submodel_alias
                        on_condition = functools.reduce(
                            operator.and_,
                            [
                                getattr(model_or_alias, model_column.name)
                                == getattr(submodel_alias, submodel_column.name)
                                for model_column, submodel_column in value.property.local_remote_pairs
                            ]
                        )
                        is_optional = any(model_column.nullable for model_column, _ in value.property.local_remote_pairs)
                        joins.append((model_or_alias, submodel_alias, on_condition, is_optional))
                        _impl(submodel_alias, (*model_path, name))
                    else:
                        continue
                else:
                    if len(model_path) > len(prefix) and name not in getattr(model, "REPR_FIELDS", []):
                        continue
                    if getattr(value, "foreign_keys", None):
                        continue
                    mapped_attrs[(*model_path, name)] = value
            elif isinstance(value, builtins.property):
                virtual_attrs[(*model_path, name)] = value
            elif name in ["metadata", "registry"]:
                continue
            else:
                raise NotImplementedError()

    _impl(model, prefix)

    # sort mapped attrs
    def comparision(value: tuple[int, tuple[tuple[str, ...], typing.Any]]):
        i, (attr_path, attr) = value
        return (-len(attr_path), i)  # longest path first, keep order of attrs on each level
    mapped_attrs_sorted = sorted(enumerate(mapped_attrs.items()), key=comparision)
    mapped_attrs = {k: v for i, (k, v) in mapped_attrs_sorted}

    return mapped_attrs, virtual_attrs, joins, submodels


def build_expression_context(
    fields: dict[tuple[str, ...], sqla.orm.attributes.QueryableAttribute],
) -> SimpleNamespace:
    """
    Build context to execution expressions in (e.g. virtual fields, where selectors, etc.)
    """
    context = SimpleNamespace(
        **{model.__name__: model for model in db.schema.get_all_models()},
        ModelRunMode=db.schema.ModelRunMode,  # TODO: cleanup
        sqla=sqla,
        func=sqla.func
    )
    for field_path, field in fields.items():
        current_context = context
        for path_part in field_path[:-1]:
            if not hasattr(current_context, path_part):
                setattr(current_context, path_part, SimpleNamespace())
            current_context = getattr(current_context, path_part)

        assert not hasattr(current_context, field_path[-1])
        setattr(current_context, field_path[-1], field)

    return context

def build_query(
    model: type[Model],
    fields: Optional[list[str]] = None,
    where: Optional[list[str]] = None,
    group_by: Optional[list[str]] = None,
    order_by: Optional[list[str]] = None,
    limit: Optional[int] = None,
    as_subquery_entities: Optional[str] = None,  # alias for the
):
    if as_subquery_entities:
        path_prefix = (as_subquery_entities,)
        alias_name = as_subquery_entities
    else:
        path_prefix = ()
        alias_name = "_model"

    # alias the model so that we can easily access it from the result
    # TODO: now that we alias all the time some parts of the expr_context will not work (e.g. IconRun.name == ...)
    model = sqla.orm.aliased(model, name=alias_name)

    mapped_attrs, virtual_attrs, joins, submodels = get_all_model_attrs(model, path_prefix)

    if virtual_attrs:
        raise NotImplementedError()

    expr_context = build_expression_context(mapped_attrs)  # TODO: attr_prefix

    accessors: dict[tuple[str, ...], typing.Callable] = {}

    group_by_attrs = {}
    if group_by:
        group_by_column_exprs = []
        for group_by_expr in group_by:
            expr = eval(group_by_expr, expr_context.__dict__)
            group_by_column_exprs.append(expr)

        for model_path, submodel in {path_prefix: model, **submodels}.items():
            # todo: test for alias
            if hasattr(submodel, "Aggregation"):
                for name, value in _get_cls_attrs(submodel.Aggregation).items():
                    attr_path = (*model_path, name)
                    if attr_path not in mapped_attrs:
                        continue
                    label = ".".join(attr_path)
                    aggregation_function = getattr(submodel.Aggregation, name)
                    group_by_attrs[attr_path] = aggregation_function(getattr(submodel, name)).label(label)
                    accessors[attr_path] = (lambda label: lambda result: getattr(result, label))(label)

        order_by_expr_context = build_expression_context(group_by_attrs)

        # sort results according to order in mapped_attrs
        group_by_attrs = _sort_dict_by_order_in_other_dict(group_by_attrs, mapped_attrs)
        accessors = _sort_dict_by_order_in_other_dict(accessors, mapped_attrs)
    else:
        order_by_expr_context = expr_context
        for attr_path in mapped_attrs.keys():
            accessors[attr_path] = (lambda attr_path: lambda result: functools.reduce(getattr, [alias_name, *attr_path[len(path_prefix):]], result))(attr_path)

    #
    # build query
    #
    # what so select
    selected_entities = [model]
    if group_by:
        selected_entities = [*selected_entities, *group_by_attrs.values()]
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
        query = query.group_by(*group_by_column_exprs)

    if limit:
        query = query.limit(limit)

    if as_subquery_entities:
        subquery = query.subquery()

        # all attributes
        if group_by:
            output_attrs = {}
            for attr_path, attr in group_by_attrs.items():
                label = '.'.join(attr_path)
                output_attrs[attr_path] = getattr(subquery.c, label).label(label)
            entities = list(output_attrs.values())
        else:
            new_model = sqla.orm.aliased(model, subquery, name=alias_name)
            output_attrs, _, _, submodels = get_all_model_attrs(new_model, path_prefix, subquery)
            entities = [new_model, *submodels.values()]
    else:
        if group_by:
            output_attrs = group_by_attrs
        else:
            output_attrs = mapped_attrs

    # filter what attributes we output
    if fields:
        parsed_fields = []
        for field in fields:
            for attr_path in output_attrs.keys():
                if _attr_path_starts_with(attr_path, tuple(field.split("."))):
                    parsed_fields.append(attr_path)
        output_attrs = {k: v for k, v in output_attrs.items() if k in parsed_fields}

    if as_subquery_entities:
        return entities, output_attrs, accessors
    else:
        return query, output_attrs, accessors