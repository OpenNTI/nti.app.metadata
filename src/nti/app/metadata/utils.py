#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

try:
    from BTrees.check import check as btree_check
except ImportError:
    def btree_check(unused_item):
        pass

from zope import component

from zope.index.topic import TopicIndex

from zope.index.topic.interfaces import ITopicFilteredSet

from zope.intid.interfaces import IIntIds

from zc.catalog.index import NormalizationWrapper

from zc.catalog.interfaces import IIndexValues

from zope.mimetype.interfaces import IContentTypeAware

from ZODB.POSException import POSError

from nti.dataserver.metadata.utils import queryId
from nti.dataserver.metadata.utils import get_principal_metadata_objects

from nti.externalization.interfaces import LocatedExternalDict

from nti.externalization.oids import to_external_oid

from nti.zope_catalog.catalog import isBroken

from nti.zope_catalog.interfaces import IKeywordIndex
from nti.zope_catalog.interfaces import IMetadataCatalog

logger = __import__('logging').getLogger(__name__)


def parse_mimeType(obj):
    return getattr(obj, 'mimeType', None) or getattr(obj, 'mime_type', None)


def get_mime_type(obj, default='unknown'):
    result = parse_mimeType(obj)
    if not result:
        obj = IContentTypeAware(obj, None)
        result = parse_mimeType(obj)
    return str(result) if result else default


def principal_metadata_objects(principal, accept=(), intids=None):
    intids = component.getUtility(IIntIds) if intids is None else intids
    for obj in get_principal_metadata_objects(principal):
        mime_type = get_mime_type(obj)
        if accept and mime_type not in accept:
            continue
        iid = queryId(obj, intids=intids)
        if iid is not None:
            yield iid, mime_type, obj


def _set_library_catalog(catalogs):
    try:
        from nti.contentlibrary.indexed_data import get_library_catalog
        catalog = get_library_catalog()
        if catalog is not None:
            catalogs.append(catalog)
    except ImportError:
        pass


def check_indices(catalog_interface=IMetadataCatalog, intids=None,
                  test_broken=False, inspect_btrees=False, inspect_treesets=False):
    seen = set()
    broken = dict()
    result = LocatedExternalDict()
    missing = result['Missing'] = set()
    intids = component.getUtility(IIntIds) if intids is None else intids

    # get all catalogs
    catalogs = list(component.getAllUtilitiesRegisteredFor(catalog_interface))
    _set_library_catalog(catalogs)

    def _unindex(catalogs, docid):
        for catalog in catalogs:
            catalog.unindex_doc(docid)

    def _process_ids(catalogs, docids, missing, broken, seen):
        result = set()
        for uid in docids:
            if uid in seen:
                continue
            seen.add(uid)
            try:
                obj = intids.queryObject(uid)
                if obj is None:
                    result.add(uid)
                    _unindex(catalogs, uid)
                    missing.add(uid)
                elif test_broken and isBroken(obj):
                    result.add(uid)
                    _unindex(catalogs, uid)
                    broken[uid] = str(type(obj))
            except (POSError, TypeError):
                result.add(uid)
                _unindex(catalogs, uid)
                broken[uid] = str(type(obj))
            except (AttributeError):
                pass
        return result

    def _check_btree(name, item):
        logger.info("---------> %s, %s", name,
                    to_external_oid(item) or '')
        if hasattr(item, "_check"):
            item._check()  # pylint: disable=protected-access
        btree_check(item)

    def _check_values_to_documents(name, btree):
        _check_btree(name, btree)
        if inspect_treesets:
            for key in btree.keys():
                logger.info("\t---> %r, %s", key,
                            to_external_oid(key) or '')
                value = btree[key]
                if hasattr(value, "_check"):
                    value._check()  # pylint: disable=protected-access
                btree_check(value)

    def _check_btrees(name, index):
        logger.info("---> Checking %s, %s", name, index.__class__)
        index = getattr(index, 'index', index)
        try:
            btree = getattr(index, 'documents_to_values', None)
            if btree is not None:
                _check_btree('documents_to_values', btree)
            btree = getattr(index, 'values_to_documents', None)
            if btree is not None:
                _check_values_to_documents('values_to_documents', btree)
        except Exception as e:
            logger.exception(e)
            raise e

    def _process_catalog(catalog):
        logger.info("Processing %s-[%s]",
                    getattr(catalog, '__name__', None),
                    catalog.__class__)
        for name, index in catalog.items():
            if isinstance(index, NormalizationWrapper):
                index = index.index
            try:
                if IIndexValues.providedBy(index):
                    if inspect_btrees:
                        _check_btrees(name, index)
                    docids = list(index.ids())
                    processed = _process_ids(catalogs, docids,
                                             missing, broken, seen)
                    if processed:
                        logger.info("%s record(s) unindexed. Source %s,%s",
                                    len(processed), name, catalog)
                elif IKeywordIndex.providedBy(index):
                    if inspect_btrees:
                        _check_btrees(name, index)
                    docids = list(index.ids())
                    processed = _process_ids(catalogs, docids,
                                             missing, broken, seen)
                    if processed:
                        logger.info("%s record(s) unindexed. Source %s,%s",
                                    len(processed), name, catalog)
                elif isinstance(index, TopicIndex):
                    # pylint: disable=protected-access
                    for filter_index in index._filters.values():
                        if ITopicFilteredSet.providedBy(filter_index):
                            docids = list(filter_index.getIds())
                            processed = _process_ids(catalogs, docids,
                                                     missing, broken, seen)
                            if processed:
                                logger.info("%s record(s) unindexed. Source %s,%s",
                                            len(processed), name, catalog)
            except (POSError, TypeError):
                logger.error('Errors getting ids from index "%s" (%s) in catalog %s',
                             name, index, catalog)

    for catalog in catalogs:
        _process_catalog(catalog)

    result['Missing'] = sorted(missing)
    result['TotalIndexed'] = len(seen)
    result['TotalMissing'] = len(missing)
    if test_broken:
        result['Broken'] = broken
        result['TotalBroken'] = len(broken)
    return result
