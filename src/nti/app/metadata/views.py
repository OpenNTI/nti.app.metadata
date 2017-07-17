#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import six

from requests.structures import CaseInsensitiveDict

from zope import component
from zope import interface

from zope.cachedescriptors.property import Lazy

from zope.catalog.interfaces import ICatalog

from zope.container.contained import Contained

from zope.intid.interfaces import IIntIds

from zope.traversing.interfaces import IPathAdapter

from ZODB.POSException import POSError

from zc.catalog.interfaces import IValueIndex

from pyramid import httpexceptions as hexc

from pyramid.view import view_config
from pyramid.view import view_defaults

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.metadata.utils import check_indices

from nti.app.metadata.reindexer import reindex

from nti.common.string import is_true

from nti.dataserver import authorization as nauth

from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout
from nti.dataserver.interfaces import IUserGeneratedData

from nti.dataserver.metadata.index import IX_CREATOR
from nti.dataserver.metadata.index import IX_MIMETYPE
from nti.dataserver.metadata.index import IX_SHAREDWITH
from nti.dataserver.metadata.index import IX_CONTAINERID
from nti.dataserver.metadata.index import get_metadata_catalog

from nti.dataserver.users import User

from nti.dataserver.sharing import SharingContextCache

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.metadata import QUEUE_NAMES

from nti.metadata import queue_add
from nti.metadata import metadata_catalogs

from nti.metadata.processing import get_job_queue

from nti.ntiids.ntiids import find_object_with_ntiid, is_valid_ntiid_string

from nti.zope_catalog.interfaces import IMetadataCatalog

ITEMS = StandardExternalFields.ITEMS
LINKS = StandardExternalFields.LINKS
TOTAL = StandardExternalFields.TOTAL
MIMETYPE = StandardExternalFields.MIMETYPE
ITEM_COUNT = StandardExternalFields.ITEM_COUNT


@interface.implementer(IPathAdapter)
class MetadataPathAdapter(Contained):

    __name__ = 'metadata'

    def __init__(self, context, request):
        self.context = context
        self.request = request
        self.__parent__ = context


def _make_min_max_btree_range(search_term):
    min_inclusive = search_term  # start here
    max_exclusive = search_term[0:-1] + six.unichr(ord(search_term[-1]) + 1)
    return min_inclusive, max_exclusive


def username_search(search_term):
    min_inclusive, max_exclusive = _make_min_max_btree_range(search_term)
    dataserver = component.getUtility(IDataserver)
    users = IShardLayout(dataserver).users_folder
    usernames = list(users.iterkeys(min_inclusive,
                                    max_exclusive,
                                    excludemax=True))
    return usernames


@view_config(name='MimeTypes')
@view_config(name='mime_types')
@view_defaults(route_name='objects.generic.traversal',
               name='mime_types',
               renderer='rest',
               request_method='GET',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class GetMimeTypesView(AbstractAuthenticatedView):

    def __call__(self):
        mime_types = set()
        catalogs = metadata_catalogs()
        for catalog in catalogs:
            if not IX_MIMETYPE in catalog:
                continue
            index = catalog[IX_MIMETYPE]
            if not IValueIndex.providedBy(index):
                continue
            mime_types.update(index.values_to_documents.keys())
        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        items = result[ITEMS] = sorted(mime_types)
        result[ITEM_COUNT] = result[TOTAL] = len(items)
        return result


@view_config(name='Reindexer')
@view_config(name='reindexer')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class ReindexerView(AbstractAuthenticatedView,
                             ModeledContentUploadRequestUtilsMixin):

    def readInput(self, value=None):
        result = CaseInsensitiveDict()
        if self.request.body:
            values = super(ReindexerView, self).readInput(value=value)
            result.update(**values)
        return result

    def _do_call(self):
        values = self.readInput()
        term = values.get('term') or values.get('search')
        system = values.get('system') or values.get('systemUser')
        usernames = values.get('usernames') or values.get('username')

        # user search
        if term:
            usernames = username_search(term)
        elif usernames and isinstance(usernames, six.string_types):
            usernames = usernames.split(',')
        else:
            usernames = ()

        accept = values.get('accept') or values.get('mimeTypes') or u''
        accept = set(accept.split(',')) if accept else ()
        if accept and '*/*' not in accept:
            accept = set(accept)
        else:
            accept = ()

        result = reindex(accept=accept,
                         usernames=usernames,
                         system=is_true(system))
        return result


@view_config(name='IndexUserGeneratedData')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class IndexUserGeneratedDataView(AbstractAuthenticatedView,
                                 ModeledContentUploadRequestUtilsMixin):

    def _do_call(self):
        total = 0
        count = 0
        indexed = 0
        intids = component.getUtility(IIntIds)
        catalog = get_metadata_catalog()
        if catalog is not None:
            mimeTypeIdx = catalog[IX_MIMETYPE]
            total = len(mimeTypeIdx.ids())
            logger.info('Indexing new extent (count=%s)', total)
            for uid in mimeTypeIdx.ids():
                count += 1
                if count % 5000 == 0:
                    logger.info('Indexing new extent (%s/%s)', count, total)
                obj = intids.queryObject(uid)
                try:
                    if IUserGeneratedData.providedBy(obj):
                        queue_add(obj)
                except (TypeError, POSError):
                    pass
        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        result[TOTAL] = total
        result[ITEM_COUNT] = indexed
        return result


@view_config(name='CheckIndices')
@view_config(name='check_indices')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class CheckIndicesView(AbstractAuthenticatedView,
                       ModeledContentUploadRequestUtilsMixin):

    @Lazy
    def intids(self):
        return component.getUtility(IIntIds)

    def readInput(self, value=None):
        if self.request.body:
            values = super(CheckIndicesView, self).readInput(value)
            result = CaseInsensitiveDict(values)
        else:
            values = self.request.params
            result = CaseInsensitiveDict(values)
        return result

    def __call__(self):
        values = self.readInput()
        all_catalog = is_true(values.get('all'))
        test_broken = is_true(values.get('broken'))
        check_btrees = is_true(values.get('check'))
        if all_catalog:
            catalog_interface = ICatalog
        else:
            catalog_interface = IMetadataCatalog
        result = check_indices(catalog_interface=catalog_interface,
                               test_broken=test_broken,
                               intids=self.intids,
                               inspect_btrees=check_btrees)
        return result


@view_config(name='UnindexDoc')
@view_config(name='unindex_doc')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class UnindexDocView(AbstractAuthenticatedView):

    @Lazy
    def intids(self):
        return component.getUtility(IIntIds)

    @Lazy
    def catalogs(self):
        result = {
            c.__name__: c for c in component.getAllUtilitiesRegisteredFor(ICatalog)
        }
        try:
            from nti.contentlibrary.indexed_data import get_library_catalog
            catalog = get_library_catalog()
            result[catalog.__name__] = catalog
        except ImportError:
            pass
        return result

    def __call__(self):
        request = self.request
        doc_id = request.subpath[0] if request.subpath else ''
        try:
            doc_id = int(doc_id)
        except (ValueError, TypeError):
            raise_json_error(self.request,
                             hexc.HTTPUnprocessableEntity,
                             {
                                'message': u"Invalid/Missing document id.",
                             },
                             None)

        if doc_id not in self.intids:
            raise hexc.HTTPNotFound()

        for name, catalog in self.catalogs.items():
            __traceback_info = name, catalog
            logger.warn("Unindexing %s from %s", doc_id, name)
            catalog.unindex_doc(doc_id)
        return hexc.HTTPNoContent()


@view_config(name='IndexDoc')
@view_config(name='index_doc')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class IndexDocView(AbstractAuthenticatedView):

    @Lazy
    def intids(self):
        return component.getUtility(IIntIds)

    @Lazy
    def catalogs(self):
        result = {
            c.__name__: c for c in component.getAllUtilitiesRegisteredFor(ICatalog)
        }
        return result

    def find_object(self, s):
        try:
            doc_id = int(s)
        except (ValueError, TypeError):
            if not is_valid_ntiid_string(s):
                raise_json_error(self.request,
                                 hexc.HTTPUnprocessableEntity,
                                 {
                                    'message': u"Invalid document id.",
                                 },
                                 None)
            else:
                obj = find_object_with_ntiid(s)
                doc_id = self.intids.queryId(obj)
        
        obj = self.intids.queryObject(doc_id) if doc_id is not None else None
        if obj is None:
            raise hexc.HTTPNotFound()
        return obj, doc_id

    def __call__(self):
        request = self.request
        subpath = request.subpath[0] if request.subpath else ''
        obj, doc_id = self.find_object(subpath)
        for name, catalog in self.catalogs.items():
            __traceback_info = name, catalog
            logger.warn("Indexing %s to %s", doc_id, name)
            if IMetadataCatalog.providedBy(catalog):
                catalog.force_index_doc(doc_id, obj)
            else:
                catalog.index_doc(doc_id, obj)
        return hexc.HTTPNoContent()


@view_config(name='UserUGD')
@view_config(name='user_ugd')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class UGDView(AbstractAuthenticatedView):

    @Lazy
    def intids(self):
        return component.getUtility(IIntIds)

    @classmethod
    def parse_mime_types(cls, value):
        mime_types = set(value.split(',')) if value else ()
        if '*/*' in mime_types:
            mime_types = ()
        elif mime_types:
            mime_types = {e.strip().lower() for e in mime_types}
            mime_types.discard('')
        return tuple(mime_types) if mime_types else ()

    @Lazy
    def catalog(self):
        return get_metadata_catalog()

    def get_owned(self, user, ntiid, mime_types=()):
        username = user.username
        query = {IX_CONTAINERID: {'any_of': (ntiid,)},
                 IX_CREATOR: {'any_of': (username,)}}
        if mime_types:
            query[IX_MIMETYPE] = {'any_of': mime_types}
        result = self.catalog.apply(query) or self.catalog.family.IF.LFSet()
        return result

    def get_shared_container(self, user, ntiid, mime_types=()):
        username = user.username
        query = {IX_CONTAINERID: {'any_of': (ntiid,)},
                 IX_SHAREDWITH: {'any_of': (username,)}}
        if mime_types:
            query[IX_MIMETYPE] = {'any_of': mime_types}
        result = self.catalog.apply(query) or self.catalog.family.IF.LFSet()
        return result

    def get_shared(self, user, ntiid, mime_types=()):
        # start w/ user
        result = [self.get_shared_container(user, ntiid, mime_types)]
        creator_index = self.catalog[IX_CREATOR].index

        # process communities followed
        context_cache = SharingContextCache()
        context_cache._build_entities_followed_for_read(user)
        persons_following = context_cache.persons_followed
        communities_seen = context_cache.communities_followed
        for following in communities_seen:
            if following == user:
                continue

            sink = self.catalog.family.IF.LFSet()
            uids = self.get_shared_container(following, ntiid, mime_types)
            for uid, username in creator_index.zip(uids):
                creator = User.get_user(username) if username else None
                if creator and not user.is_ignoring_shared_data_from(creator):
                    sink.add(uid)
            result.append(sink)

        # process other dynamic sharing targets
        for comm in context_cache(user._get_dynamic_sharing_targets_for_read):
            if comm in communities_seen:
                continue

            sink = self.catalog.family.IF.LFSet()
            uids = self.get_shared_container(comm, ntiid, mime_types)
            for uid, username in creator_index.zip(uids):
                creator = User.get_user(username) if username else None
                if creator in persons_following or creator is user:
                    sink.add(uid)
            result.append(sink)

        result = self.catalog.family.IF.multiunion(result)
        return result

    def get_ids(self, user, ntiid, mime_types=()):
        owned = self.get_owned(user, ntiid, mime_types)
        shared = self.get_shared(user, ntiid, mime_types)
        result = self.catalog.family.IF.union(owned, shared)
        return result

    def query_objects(self, uids=()):
        for doc_id in uids or ():
            obj = self.intids.queryObject(doc_id)
            if obj is not None:
                yield obj

    def readInput(self):
        result = CaseInsensitiveDict(self.request.params)
        return result

    def __call__(self):
        values = self.readInput()
        username = values.get('user') or values.get('username')
        user = User.get_user(username or '')
        if user is None:
            raise_json_error(self.request,
                             hexc.HTTPUnprocessableEntity,
                             {
                                'message': u"'Provide a valid user.",
                                'field': 'username',
                             },
                             None)

        ntiid = values.get('ntiid') or values.get('containerId')
        if not ntiid:
            raise_json_error(self.request,
                             hexc.HTTPUnprocessableEntity,
                             {
                                'message': u"'Provide a valid container.",
                                'field': 'ntiid',
                             },
                             None)

        mime_types = values.get('mime_types') or values.get('mimeTypes') or u''
        mime_types = self.parse_mime_types(mime_types)

        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        uids = self.get_ids(user, ntiid, mime_types)
        items = result[ITEMS] = list(self.query_objects(uids))
        result[ITEM_COUNT] = result[TOTAL] = len(items)
        return result


# queue views


@view_config(name='Jobs')
@view_config(name='jobs')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class QueueJobsView(AbstractAuthenticatedView):

    def __call__(self):
        total = 0
        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        items = result[ITEMS] = {}
        for name in QUEUE_NAMES:
            queue = get_job_queue(name)
            items[name] = list(queue.keys())  # snapshopt
            total += len(items[name])
        result[TOTAL] = result[ITEM_COUNT] = total
        return result


@view_config(name='EmptyQueues')
@view_config(name='empty_queues')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class EmptyQueuesView(AbstractAuthenticatedView):

    def __call__(self):
        for name in QUEUE_NAMES:
            queue = get_job_queue(name)
            queue.empty()
        return hexc.HTTPNoContent()
