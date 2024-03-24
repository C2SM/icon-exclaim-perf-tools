import builtins
import functools
import inspect
import operator
from types import SimpleNamespace
import typing

import sqlalchemy as sqla
import sqlalchemy.orm.util
import sqlalchemy.sql.elements

from icon_exclaim_perf_tools import db

ModelOrAlias = db.schema.Model | sqla.orm.util.AliasedClass[db.schema.Model]

def _get_cls_attrs(cls):
    attrs = [*vars(cls).keys()]

    # some attrs are only declared as an annotation and then defined by the base class
    #  in order to keep the ordering sort by appearence in the annotation dict
    annotations_ = {attr: i for i, attr in enumerate(cls.__annotations__.keys())}
    def comparision(left: str, right: str):
        return annotations_.get(left, -1)-annotations_.get(right, -1)
    attrs.sort(key=functools.cmp_to_key(comparision))

    return attrs


def _is_model(arg: typing.Any):
    if isinstance(arg, sqla.orm.util.AliasedClass):
        arg = sqla.inspect(arg).class_
    return inspect.isclass(arg) and issubclass(arg, db.schema.Model)


def get_all_model_attrs(model: type[db.schema.Model]):
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
        prefix: tuple[str, ...]
    ):
        if isinstance(model_or_alias, sqla.orm.util.AliasedClass):
            model = sqla.inspect(model_or_alias).class_
        else:
            model = model_or_alias
        assert issubclass(model, db.schema.Model)

        if model in ignored_models:
            return  # todo
        ignored_models.add(model)

        for name in _get_cls_attrs(model):  # todo: check vars or __dict__ since different if inheriting
            value = getattr(model_or_alias, name)

            if name.startswith("_") or name[0].isupper():
                continue
            if isinstance(value, sqla.orm.attributes.QueryableAttribute):
                if isinstance(value.property, sqla.orm.relationships.Relationship):
                    if value.property.direction == sqla.orm.relationships.RelationshipDirection.MANYTOONE:
                        submodel = value.property.entity.class_
                        submodel_alias = sqla.orm.aliased(submodel)
                        submodels[(*prefix, name)] = submodel_alias
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
                        _impl(submodel_alias, (*prefix, name))
                    else:
                        continue
                else:
                    if len(prefix) > 0 and name not in getattr(model, "REPR_FIELDS", []):
                        continue
                    if getattr(value, "foreign_keys", None):
                        continue
                    mapped_attrs[(*prefix, name)] = value
            elif isinstance(value, builtins.property):
                virtual_attrs[(*prefix, name)] = value
            else:
                raise NotImplementedError()

    _impl(model, [])

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