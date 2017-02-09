#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import six
import time

from requests.structures import CaseInsensitiveDict

from zope import component
from zope import interface

from zope.catalog.interfaces import ICatalog

from zope.container.contained import Contained

from zope.intid.interfaces import IIntIds

from zope.security.management import system_user

from zope.traversing.interfaces import IPathAdapter

from ZODB.POSException import POSError

from zc.catalog.interfaces import IValueIndex

from pyramid import httpexceptions as hexc

from pyramid.view import view_config
from pyramid.view import view_defaults

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.metadata.utils import check_indices
from nti.app.metadata.utils import find_principal_metadata_objects

from nti.app.metadata.reindexer import reindex

from nti.common.string import is_true

from nti.dataserver import authorization as nauth

from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout
from nti.dataserver.interfaces import IUserGeneratedData

from nti.dataserver.metadata_index import IX_CREATOR
from nti.dataserver.metadata_index import IX_MIMETYPE
from nti.dataserver.metadata_index import IX_SHAREDWITH
from nti.dataserver.metadata_index import IX_CONTAINERID

from nti.dataserver.users import User

from nti.dataserver.sharing import SharingContextCache

from nti.externalization.externalization import to_external_object
from nti.externalization.externalization import NonExternalizableObjectError

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.metadata import metadata_queue
from nti.metadata import metadata_catalogs
from nti.metadata import dataserver_metadata_catalog

from nti.metadata.reactor import process_queue

from nti.metadata.interfaces import DEFAULT_QUEUE_LIMIT

from nti.ntiids.ntiids import find_object_with_ntiid

from nti.property.property import Lazy

from nti.zope_catalog.catalog import ResultSet
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
    max_exclusive = search_term[0:-1] + unichr(ord(search_term[-1]) + 1)
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
        items = result[ITEMS] = sorted(mime_types)
        result[ITEM_COUNT] = result[TOTAL] = len(items)
        return result


@view_config(name='GetMetadataObjects')
@view_config(name='get_metadata_objects')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class GetMetadataObjectsView(AbstractAuthenticatedView):

    def readInput(self, value=None):
        result = CaseInsensitiveDict(self.request.params)
        return result

    def __call__(self):
        values = self.readInput()
        username = values.get('user') or values.get('username')
        system = is_true(values.get('system') or values.get('systemUser'))
        if system:
            principal = system_user()
        elif username:
            principal = User.get_user(username)
        else:
            raise hexc.HTTPUnprocessableEntity('Must specify a principal')

        if principal is None:
            raise hexc.HTTPUnprocessableEntity(
                'Cannot find the specified user')

        accept = values.get('accept') or values.get('mimeTypes') or u''
        accept = set(accept.split(',')) if accept else ()
        if accept and '*/*' not in accept:
            accept = set(accept)
        else:
            accept = ()

        result = LocatedExternalDict()
        items = result[ITEMS] = {}
        for iid, mimeType, obj in find_principal_metadata_objects(principal, accept):
            try:
                ext_obj = to_external_object(obj, decorate=False)
                items[iid] = ext_obj
            except Exception:
                items[iid] = {'Class': 'NonExternalizableObject',
                              'InternalType': str(type(obj)),
                              'MIMETYPE': mimeType}
        result[ITEM_COUNT] = result[TOTAL] = len(items)
        return result


@view_config(name='ReindexUserObjects')
@view_config(name='reindex_user_objects')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class ReindexUserObjectsView(AbstractAuthenticatedView,
                             ModeledContentUploadRequestUtilsMixin):

    def readInput(self, value=None):
        result = CaseInsensitiveDict()
        if self.request.body:
            values = super(ReindexUserObjectsView, self).readInput(value=value)
            result.update(**values)
        return result

    def _do_call(self):
        values = self.readInput()
        queue_limit = values.get('limit', None)
        term = values.get('term') or values.get('search')
        all_users = values.get('all') or values.get('allUsers')
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

        # queue limit
        if queue_limit is not None:
            try:
                queue_limit = int(queue_limit)
                assert queue_limit > 0 or queue_limit == -1
            except (ValueError, AssertionError):
                raise hexc.HTTPUnprocessableEntity('invalid queue size')

        result = reindex(accept=accept,
                         usernames=usernames,
                         system=is_true(system),
                         queue_limit=queue_limit,
                         all_users=is_true(all_users))
        return result


@view_config(name='Reindex')
@view_config(name='reindex')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class ReindexView(AbstractAuthenticatedView,
                  ModeledContentUploadRequestUtilsMixin):

    def readInput(self, value=None):
        if self.request.body:
            result = super(ReindexView, self).readInput(value=value)
            result = CaseInsensitiveDict(result)
        else:
            result = CaseInsensitiveDict(self.request.params)
        return result

    @property
    def intids(self):
        return component.getUtility(IIntIds)

    def _do_call(self):
        values = self.readInput()
        ntiids = values.get('ntiid') or values.get('ntiids')
        if isinstance(ntiids, six.string_types):
            ntiids = ntiids.split()
        if not ntiids:
            raise hexc.HTTPUnprocessableEntity('Must specify a valid NTIID.')

        all_catalog = is_true(values.get('all'))
        if all_catalog:
            catalog_interface = ICatalog
        else:
            catalog_interface = IMetadataCatalog
        catalogs = [x for _, x in component.getUtilitiesFor(catalog_interface)]

        result = LocatedExternalDict()
        items = result[ITEMS] = {}
        for ntiid in set(ntiids):
            obj = find_object_with_ntiid(ntiid)
            doc_id = self.intids.queryId(obj)
            if doc_id is None:
                continue
            items[ntiid] = doc_id
            for catalog in catalogs:
                if IMetadataCatalog.providedBy(catalog):
                    catalog.force_index_doc(doc_id, obj)
                else:
                    catalog.index_doc(doc_id, obj)
        result[TOTAL] = result[ITEM_COUNT] = len(items)
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
        queue = metadata_queue()
        intids = component.getUtility(IIntIds)
        catalog = dataserver_metadata_catalog()
        if queue is not None and catalog is not None:
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
                        try:
                            queue.add(uid)
                            indexed += 1
                        except TypeError:
                            pass
                except (TypeError, POSError):
                    pass
        result = LocatedExternalDict()
        result[TOTAL] = total
        result[ITEM_COUNT] = indexed
        return result


@view_config(name='ProcessQueue')
@view_config(name='process_queue')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class ProcessQueueView(AbstractAuthenticatedView,
                       ModeledContentUploadRequestUtilsMixin):

    def readInput(self):
        result = {}
        if self.request.body:
            values = super(ProcessQueueView, self).readInput()
            result = CaseInsensitiveDict(values)
        return result

    def _do_call(self):
        values = self.readInput()
        limit = values.get('limit', DEFAULT_QUEUE_LIMIT)
        try:
            limit = int(limit)
            assert limit > 0 or limit == -1
        except (ValueError, AssertionError):
            raise hexc.HTTPUnprocessableEntity('invalid limit size')

        now = time.time()
        total = process_queue(limit=limit)
        result = LocatedExternalDict()
        result['Elapsed'] = time.time() - now
        result[TOTAL] = total
        return result


@view_config(name='QueuedObjects')
@view_config(name='queued_objects')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class QueuedObjectsView(AbstractAuthenticatedView):

    def __call__(self):
        intids = component.getUtility(IIntIds)
        catalog_queue = metadata_queue()
        result = LocatedExternalDict()
        items = result[ITEMS] = {}
        for key in list(catalog_queue.keys()):
            try:
                obj = intids.queryObject(key)
                if obj is not None:
                    items[key] = to_external_object(obj)
            except NonExternalizableObjectError:
                items[key] = {'Object': str(type(obj))}
            except Exception as e:
                items[key] = {'Message': str(e),
                              'Object': str(type(obj)),
                              'Exception': str(type(e))}
        result[ITEM_COUNT] = result[TOTAL] = len(items)
        return result


@view_config(name='SyncQueue')
@view_config(name='sync_queue')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class SyncQueueView(AbstractAuthenticatedView,
                    ModeledContentUploadRequestUtilsMixin):

    def __call__(self):
        catalog_queue = metadata_queue()
        if catalog_queue.syncQueue():
            logger.info("Queue synched")
        return hexc.HTTPNoContent()


@view_config(name='CheckIndices')
@view_config(name='check_indices')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class CheckIndicesView(AbstractAuthenticatedView,
                       ModeledContentUploadRequestUtilsMixin):

    @property
    def intids(self):
        return component.getUtility(IIntIds)

    def readInput(self):
        if self.request.body:
            values = super(CheckIndicesView, self).readInput()
            result = CaseInsensitiveDict(values)
        else:
            values = self.request.params
            result = CaseInsensitiveDict(values)
        return result

    def __call__(self):
        values = self.readInput()
        all_catalog = is_true(values.get('all'))
        test_broken = is_true(values.get('broken'))
        if all_catalog:
            catalog_interface = ICatalog
        else:
            catalog_interface = IMetadataCatalog
        result = check_indices(catalog_interface=catalog_interface,
                               test_broken=test_broken,
                               intids=self.intids)
        return result


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
            mime_types.discard(u'')
        return tuple(mime_types) if mime_types else ()

    @Lazy
    def catalog(self):
        return dataserver_metadata_catalog()

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

    def readInput(self, value=None):
        result = CaseInsensitiveDict(self.request.params)
        return result

    def __call__(self):
        values = self.readInput()
        username = values.get('user') or values.get('username')
        user = User.get_user(username or '')
        if user is None:
            raise hexc.HTTPUnprocessableEntity('Provide a valid user')

        ntiid = values.get('ntiid') or values.get('containerId')
        if not ntiid:
            raise hexc.HTTPUnprocessableEntity('Provide a valid container')

        mime_types = values.get('mime_types') or values.get('mimeTypes') or u''
        mime_types = self.parse_mime_types(mime_types)

        result = LocatedExternalDict()
        uids = self.get_ids(user, ntiid, mime_types)
        items = result[ITEMS] = [x for x in ResultSet(uids, self.intids, True)]
        result[ITEM_COUNT] = result[TOTAL] = len(items)
        return result
