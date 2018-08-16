#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from pyramid import httpexceptions as hexc

from pyramid.view import view_config
from pyramid.view import view_defaults

from requests.structures import CaseInsensitiveDict

import six

from zc.catalog.interfaces import IValueIndex

from zope import component
from zope import interface

from zope.cachedescriptors.property import Lazy

from zope.catalog.interfaces import ICatalogEdit

from zope.intid.interfaces import IIntIds

from zope.location.interfaces import IContained

from zope.traversing.interfaces import IPathAdapter

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.metadata.reindexer import reindex
from nti.app.metadata.reindexer import rebuild_metadata_catalog

from nti.app.metadata.utils import check_indices

from nti.common.string import is_true

from nti.dataserver import authorization as nauth

from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout

from nti.dataserver.metadata.index import IX_MIMETYPE

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.metadata import QUEUE_NAMES

from nti.metadata import metadata_catalogs

from nti.metadata.processing import get_job_queue

from nti.ntiids.ntiids import is_valid_ntiid_string
from nti.ntiids.ntiids import find_object_with_ntiid

from nti.zope_catalog.interfaces import IDeferredCatalog

ITEMS = StandardExternalFields.ITEMS
LINKS = StandardExternalFields.LINKS
TOTAL = StandardExternalFields.TOTAL
MIMETYPE = StandardExternalFields.MIMETYPE
ITEM_COUNT = StandardExternalFields.ITEM_COUNT

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IPathAdapter, IContained)
class MetadataPathAdapter(object):

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
    # pylint: disable=no-member
    usernames = tuple(users.iterkeys(min_inclusive,
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
            catalog_interface = ICatalogEdit
        else:
            catalog_interface = IDeferredCatalog
        result = check_indices(catalog_interface=catalog_interface,
                               test_broken=test_broken,
                               intids=self.intids,
                               inspect_btrees=check_btrees)
        return result


class IndexDocMixin(AbstractAuthenticatedView):

    @Lazy
    def intids(self):
        return component.getUtility(IIntIds)

    @Lazy
    def catalogs(self):
        result = {
            c.__name__: c for c in component.getAllUtilitiesRegisteredFor(ICatalogEdit)
        }
        return result

    def find_object(self, s):
        # pylint: disable=no-member
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


@view_config(name='UnindexDoc')
@view_config(name='unindex_doc')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=MetadataPathAdapter,
               permission=nauth.ACT_NTI_ADMIN)
class UnindexDocView(IndexDocMixin):

    def __call__(self):
        request = self.request
        subpath = request.subpath[0] if request.subpath else ''
        _, doc_id = self.find_object(subpath)
        for name, catalog in self.catalogs.items():  # pylint: disable=no-member
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
class IndexDocView(IndexDocMixin):

    def __call__(self):
        request = self.request
        subpath = request.subpath[0] if request.subpath else ''
        obj, doc_id = self.find_object(subpath)
        for name, catalog in self.catalogs.items():  # pylint: disable=no-member
            __traceback_info = name, catalog
            logger.warn("Indexing %s to %s", doc_id, name)
            catalog.index_doc(doc_id, obj)
        return hexc.HTTPNoContent()


@view_config(context=MetadataPathAdapter)
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               name="RebuildMetadataCatalog",
               permission=nauth.ACT_NTI_ADMIN)
class RebuildMetadataCatalogView(AbstractAuthenticatedView):

    def __call__(self):
        count = rebuild_metadata_catalog()
        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        result[ITEM_COUNT] = result[TOTAL] = count
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
