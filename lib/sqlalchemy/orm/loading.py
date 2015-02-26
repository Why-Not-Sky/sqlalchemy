# orm/loading.py
# Copyright (C) 2005-2014 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""private module containing functions used to convert database
rows into object instances and associated state.

the functions here are called primarily by Query, Mapper,
as well as some of the attribute loading strategies.

"""
from __future__ import absolute_import

from .. import util
from . import attributes, exc as orm_exc
from .base import _IdxLoader
from ..sql import util as sql_util
from .util import _none_set, state_str
from .. import exc as sa_exc
import collections

_new_runid = util.counter()


def instances(query, cursor, context):
    """Return an ORM result as an iterator."""

    context.runid = _new_runid()

    filter_fns = [ent.filter_fn for ent in query._entities]
    filtered = id in filter_fns

    single_entity = len(query._entities) == 1 and \
        query._entities[0].supports_single_entity

    if filtered:
        if single_entity:
            filter_fn = id
        else:
            def filter_fn(row):
                return tuple(fn(x) for x, fn in zip(row, filter_fns))

    if context._predefined_statement:
        # if the Query didn't actually build the statement,
        # we use the result set to determine where the columns
        # we're looking for are located.
        context._setup_column_processors(
            [
                (cursor._index_of(col), col)
                for col in context.primary_columns + context.secondary_columns
            ]
        )

    try:
        (labels, process) = list(zip(*context.loaders))

        if not single_entity:
            # TODO: this should be in context, so it can also be cached.
            keyed_tuple = util.lightweight_named_tuple('result', labels)

        while True:
            context.partials = {}

            if query._yield_per:
                fetch = cursor.fetchmany(query._yield_per)
                if not fetch:
                    break
            else:
                fetch = cursor.fetchall()

            if single_entity:
                proc = process[0]
                rows = [proc(row) for row in fetch]
            else:
                rows = [keyed_tuple([proc(row) for proc in process])
                        for row in fetch]

            if filtered:
                rows = util.unique_list(rows, filter_fn)

            for row in rows:
                yield row

            if not query._yield_per:
                break
    except Exception as err:
        cursor.close()
        util.raise_from_cause(err)


@util.dependencies("sqlalchemy.orm.query")
def merge_result(querylib, query, iterator, load=True):
    """Merge a result into this :class:`.Query` object's Session."""

    session = query.session
    if load:
        # flush current contents if we expect to load data
        session._autoflush()

    autoflush = session.autoflush
    try:
        session.autoflush = False
        single_entity = len(query._entities) == 1
        if single_entity:
            if isinstance(query._entities[0], querylib._MapperEntity):
                result = [session._merge(
                    attributes.instance_state(instance),
                    attributes.instance_dict(instance),
                    load=load, _recursive={})
                    for instance in iterator]
            else:
                result = list(iterator)
        else:
            mapped_entities = [i for i, e in enumerate(query._entities)
                               if isinstance(e, querylib._MapperEntity)]
            result = []
            keys = [ent._label_name for ent in query._entities]
            keyed_tuple = util.lightweight_named_tuple('result', keys)
            for row in iterator:
                newrow = list(row)
                for i in mapped_entities:
                    if newrow[i] is not None:
                        newrow[i] = session._merge(
                            attributes.instance_state(newrow[i]),
                            attributes.instance_dict(newrow[i]),
                            load=load, _recursive={})
                result.append(keyed_tuple(newrow))

        return iter(result)
    finally:
        session.autoflush = autoflush


def get_from_identity(session, key, passive):
    """Look up the given key in the given session's identity map,
    check the object for expired state if found.

    """
    instance = session.identity_map.get(key)
    if instance is not None:

        state = attributes.instance_state(instance)

        # expired - ensure it still exists
        if state.expired:
            if not passive & attributes.SQL_OK:
                # TODO: no coverage here
                return attributes.PASSIVE_NO_RESULT
            elif not passive & attributes.RELATED_OBJECT_OK:
                # this mode is used within a flush and the instance's
                # expired state will be checked soon enough, if necessary
                return instance
            try:
                state._load_expired(state, passive)
            except orm_exc.ObjectDeletedError:
                session._remove_newly_deleted([state])
                return None
        return instance
    else:
        return None


def load_on_ident(query, key,
                  refresh_state=None, lockmode=None,
                  only_load_props=None):
    """Load the given identity key from the database."""

    if key is not None:
        ident = key[1]
    else:
        ident = None

    if refresh_state is None:
        q = query._clone()
        q._get_condition()
    else:
        q = query._clone()

    if ident is not None:
        mapper = query._mapper_zero()

        (_get_clause, _get_params) = mapper._get_clause

        # None present in ident - turn those comparisons
        # into "IS NULL"
        if None in ident:
            nones = set([
                        _get_params[col].key for col, value in
                        zip(mapper.primary_key, ident) if value is None
                        ])
            _get_clause = sql_util.adapt_criterion_to_null(
                _get_clause, nones)

        _get_clause = q._adapt_clause(_get_clause, True, False)
        q._criterion = _get_clause

        params = dict([
            (_get_params[primary_key].key, id_val)
            for id_val, primary_key in zip(ident, mapper.primary_key)
        ])

        q._params = params

    if lockmode is not None:
        version_check = True
        q = q.with_lockmode(lockmode)
    elif query._for_update_arg is not None:
        version_check = True
        q._for_update_arg = query._for_update_arg
    else:
        version_check = False

    q._get_options(
        populate_existing=bool(refresh_state),
        version_check=version_check,
        only_load_props=only_load_props,
        refresh_state=refresh_state)
    q._order_by = None

    try:
        return q.one()
    except orm_exc.NoResultFound:
        return None


def _instance_processor(
    mapper, props_toload, context, column_collection,
    query_entity, path, adapter,
    only_load_props=None, refresh_state=None,
    polymorphic_discriminator=None,
        _polymorphic_from=None,
        _polymorphic_pk_getters=None,
        _polymorphic_from_populators=None):
    """Produce a mapper level row processor callable
       which processes rows into mapped instances."""

    load_is_polymorphic = mapper.polymorphic_on is not None

    if _polymorphic_from_populators is not None:
        # if we are a subclass loader, then we use the collection
        # of populators that were already set up for us.

        populators = _polymorphic_from_populators

        for prop in props_toload:
            prop.setup_for_missing_attribute(
                context, query_entity, path, mapper,
                _polymorphic_from_populators
            )

    else:
        populators = collections.defaultdict(list)

        if load_is_polymorphic:
            per_mapper_populators = collections.defaultdict(
                lambda: collections.defaultdict(list))
            per_mapper_populators[mapper] = populators

        # establish all columns and column loaders up front
        # across subclass mappers as well.   Categorize loaders
        # into mapper-specific buckets.

        for prop in props_toload:
            prop.setup(
                context,
                query_entity,
                path,
                mapper,
                adapter,
                column_collection,
                populators=per_mapper_populators[prop.parent]
                if load_is_polymorphic and not mapper.isa(prop.parent)
                else populators,
                only_load_props=only_load_props,
            )

    if _polymorphic_pk_getters:
        pk_getters = _polymorphic_pk_getters
    else:
        pk_cols = mapper.primary_key

        if adapter:
            pk_cols = [adapter.columns[c] for c in pk_cols]

        pk_getters = [_IdxLoader()] * len(pk_cols)
        context.column_processors.extend(
            (pk_col, pk_getter.setup)
            for pk_col, pk_getter in zip(pk_cols, pk_getters)
        )

    identity_class = mapper._identity_class

    props = mapper._props.values()
    if only_load_props is not None:
        props = (p for p in props if p.key in only_load_props)

    propagate_options = context.propagate_options
    if propagate_options:
        load_path = context.query._current_path + path \
            if context.query._current_path.path else path

    session_identity_map = context.session.identity_map

    populate_existing = context.populate_existing or mapper.always_refresh
    load_evt = bool(mapper.class_manager.dispatch.load)
    refresh_evt = bool(mapper.class_manager.dispatch.refresh)
    instance_state = attributes.instance_state
    instance_dict = attributes.instance_dict
    session_id = context.session.hash_key
    version_check = context.version_check

    if refresh_state:
        refresh_identity_key = refresh_state.key
        if refresh_identity_key is None:
            # super-rare condition; a refresh is being called
            # on a non-instance-key instance; this is meant to only
            # occur within a flush()
            refresh_identity_key = \
                mapper._identity_key_from_state(refresh_state)
    else:
        refresh_identity_key = None

    if mapper.allow_partial_pks:
        is_not_primary_key = _none_set.issuperset
    else:
        is_not_primary_key = _none_set.intersection

    def _instance(row):

        # determine the state that we'll be populating
        if refresh_identity_key:
            # fixed state that we're refreshing
            state = refresh_state
            instance = state.obj()
            dict_ = instance_dict(instance)
            isnew = state.runid != context.runid
            currentload = True
            loaded_instance = False
        else:
            # look at the row, see if that identity is in the
            # session, or we have to create a new one
            identitykey = (
                identity_class,
                tuple(getter(row) for getter in pk_getters)
            )

            instance = session_identity_map.get(identitykey)

            if instance is not None:
                # existing instance
                state = instance_state(instance)
                dict_ = instance_dict(instance)

                isnew = state.runid != context.runid
                currentload = not isnew
                loaded_instance = False

                if version_check and not currentload:
                    _validate_version_id(mapper, state, dict_, row, adapter)

            else:
                # create a new instance

                # check for non-NULL values in the primary key columns,
                # else no entity is returned for the row
                if is_not_primary_key(identitykey[1]):
                    return None

                isnew = True
                currentload = True
                loaded_instance = True

                instance = mapper.class_manager.new_instance()

                dict_ = instance_dict(instance)
                state = instance_state(instance)
                state.key = identitykey

                # attach instance to session.
                state.session_id = session_id
                session_identity_map._add_unpresent(state, identitykey)

        # populate.  this looks at whether this state is new
        # for this load or was existing, and whether or not this
        # row is the first row with this identity.
        if currentload or populate_existing:
            # full population routines.  Objects here are either
            # just created, or we are doing a populate_existing

            if isnew and propagate_options:
                state.load_options = propagate_options
                state.load_path = load_path

            _populate_full(
                context, row, state, dict_, isnew,
                loaded_instance, populate_existing, populators)

            if isnew:
                if loaded_instance and load_evt:
                    state.manager.dispatch.load(state, context)
                elif refresh_evt:
                    state.manager.dispatch.refresh(
                        state, context, only_load_props)

                if populate_existing or state.modified:
                    if refresh_state and only_load_props:
                        state._commit(dict_, only_load_props)
                    else:
                        state._commit_all(dict_, session_identity_map)

        else:
            # partial population routines, for objects that were already
            # in the Session, but a row matches them; apply eager loaders
            # on existing objects, etc.
            unloaded = state.unloaded
            isnew = state not in context.partials

            if not isnew or unloaded or populators["eager"]:
                # state is having a partial set of its attributes
                # refreshed.  Populate those attributes,
                # and add to the "context.partials" collection.

                to_load = _populate_partial(
                    context, row, state, dict_, isnew,
                    unloaded, populators)

                if isnew:
                    if refresh_evt:
                        state.manager.dispatch.refresh(
                            state, context, to_load)

                    state._commit(dict_, to_load)

        return instance

    if load_is_polymorphic and not _polymorphic_from and not refresh_state:
        # if we are doing polymorphic, dispatch to a different _instance()
        # method specific to the subclass mapper
        _instance = _decorate_polymorphic_switch(
            _instance, context, mapper, props_toload,
            per_mapper_populators, path,
            polymorphic_discriminator, adapter, pk_getters)

    return _instance


def _populate_full(
        context, row, state, dict_, isnew,
        loaded_instance, populate_existing, populators):
    if isnew:
        # first time we are seeing a row with this identity.
        state.runid = context.runid

        for key, getter in populators["quick"]:
            dict_[key] = getter(row)
        if populate_existing:
            for key, set_callable in populators["expire"]:
                dict_.pop(key, None)
                if set_callable:
                    state.expired_attributes.add(key)
        else:
            for key, set_callable in populators["expire"]:
                if set_callable:
                    state.expired_attributes.add(key)
        for key, populator in populators["new"]:
            populator(state, dict_, row)
        for key, populator in populators["delayed"]:
            populator(state, dict_, row)
    else:
        # have already seen rows with this identity.
        for key, populator in populators["existing"]:
            populator(state, dict_, row)


def _populate_partial(
        context, row, state, dict_, isnew,
        unloaded, populators):
    if not isnew:
        to_load = context.partials[state]
        for key, populator in populators["existing"]:
            if key in to_load:
                populator(state, dict_, row)
    else:
        to_load = unloaded
        context.partials[state] = to_load

        for key, getter in populators["quick"]:
            if key in to_load:
                dict_[key] = getter(row)
        for key, set_callable in populators["expire"]:
            if key in to_load:
                dict_.pop(key, None)
                if set_callable:
                    state.expired_attributes.add(key)
        for key, populator in populators["new"]:
            if key in to_load:
                populator(state, dict_, row)
        for key, populator in populators["delayed"]:
            if key in to_load:
                populator(state, dict_, row)
    for key, populator in populators["eager"]:
        if key not in unloaded:
            populator(state, dict_, row)

    return to_load


def _validate_version_id(mapper, state, dict_, row, adapter):

    version_id_col = mapper.version_id_col

    if version_id_col is None:
        return

    if adapter:
        version_id_col = adapter.columns[version_id_col]

    if mapper._get_state_attr_by_column(
            state, dict_, mapper.version_id_col) != row[version_id_col]:
        raise orm_exc.StaleDataError(
            "Instance '%s' has version id '%s' which "
            "does not match database-loaded version id '%s'."
            % (state_str(state), mapper._get_state_attr_by_column(
               state, dict_, mapper.version_id_col),
               row[version_id_col]))


def _decorate_polymorphic_switch(
        instance_fn, context, mapper, props_toload,
        per_mapper_populators, path,
        polymorphic_discriminator, adapter, pk_getters):

    if polymorphic_discriminator is not None:
        polymorphic_on = polymorphic_discriminator
    else:
        polymorphic_on = mapper.polymorphic_on
    if polymorphic_on is None:
        return instance_fn

    if adapter:
        polymorphic_on = adapter.columns[polymorphic_on]

    polymorphic_getter = _IdxLoader()
    if polymorphic_discriminator is not mapper.polymorphic_on:
        context.primary_columns.append(polymorphic_on)
    context.column_processors.append(
        (polymorphic_on, polymorphic_getter.setup))

    props_setup = set(props_toload)

    def configure_subclass_mapper(discriminator):
        try:
            sub_mapper = mapper.polymorphic_map[discriminator]
        except KeyError:
            raise AssertionError(
                "No such polymorphic_identity %r is defined" %
                discriminator)
        else:
            if sub_mapper is mapper:
                return None

            populators = collections.defaultdict(list)
            for super_mapper in sub_mapper.iterate_to_root():
                mapper_populators = per_mapper_populators[super_mapper]
                for k, collection in mapper_populators.items():
                    populators[k].extend(collection)
                if super_mapper is mapper:
                    break

            keys_setup = set(p.key for p in props_setup)

            props_needed = set(
                prop for prop in sub_mapper._props.values()
            ).difference(props_setup)

            props_needed = props_needed.difference(
                p for p in props_needed if p.key in keys_setup
            )

            return _instance_processor(
                sub_mapper, props_needed, context, None, None,
                path, adapter, _polymorphic_from=mapper,
                _polymorphic_pk_getters=pk_getters,
                _polymorphic_from_populators=populators)

    polymorphic_instances = util.PopulateDict(
        configure_subclass_mapper
    )

    def polymorphic_instance(row):
        discriminator = polymorphic_getter(row)
        if discriminator is not None:
            _instance = polymorphic_instances[discriminator]
            if _instance:
                return _instance(row)
        return instance_fn(row)
    return polymorphic_instance


def load_scalar_attributes(mapper, state, attribute_names):
    """initiate a column-based attribute refresh operation."""

    # assert mapper is _state_mapper(state)
    session = state.session
    if not session:
        raise orm_exc.DetachedInstanceError(
            "Instance %s is not bound to a Session; "
            "attribute refresh operation cannot proceed" %
            (state_str(state)))

    has_key = bool(state.key)

    result = False

    if mapper.inherits and not mapper.concrete:
        statement = mapper._optimized_get_statement(state, attribute_names)
        if statement is not None:
            result = load_on_ident(
                session.query(mapper).from_statement(statement),
                None,
                only_load_props=attribute_names,
                refresh_state=state
            )

    if result is False:
        if has_key:
            identity_key = state.key
        else:
            # this codepath is rare - only valid when inside a flush, and the
            # object is becoming persistent but hasn't yet been assigned
            # an identity_key.
            # check here to ensure we have the attrs we need.
            pk_attrs = [mapper._columntoproperty[col].key
                        for col in mapper.primary_key]
            if state.expired_attributes.intersection(pk_attrs):
                raise sa_exc.InvalidRequestError(
                    "Instance %s cannot be refreshed - it's not "
                    " persistent and does not "
                    "contain a full primary key." % state_str(state))
            identity_key = mapper._identity_key_from_state(state)

        if (_none_set.issubset(identity_key) and
                not mapper.allow_partial_pks) or \
                _none_set.issuperset(identity_key):
            util.warn_limited(
                "Instance %s to be refreshed doesn't "
                "contain a full primary key - can't be refreshed "
                "(and shouldn't be expired, either).",
                state_str(state))
            return

        result = load_on_ident(
            session.query(mapper),
            identity_key,
            refresh_state=state,
            only_load_props=attribute_names)

    # if instance is pending, a refresh operation
    # may not complete (even if PK attributes are assigned)
    if has_key and result is None:
        raise orm_exc.ObjectDeletedError(state)
