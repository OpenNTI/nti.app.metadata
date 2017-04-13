#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component

from zope.index.topic import TopicIndex

from zope.index.topic.interfaces import ITopicFilteredSet

from zope.intid.interfaces import IIntIds

from zc.catalog.interfaces import IIndexValues

from zope.mimetype.interfaces import IContentTypeAware

from ZODB.POSException import POSError

from nti.contentlibrary.indexed_data import get_library_catalog

from nti.externalization.interfaces import LocatedExternalDict

from nti.metadata import get_iid
from nti.metadata import get_principal_metadata_objects

from nti.zope_catalog.catalog import isBroken

from nti.zope_catalog.interfaces import IKeywordIndex
from nti.zope_catalog.interfaces import IMetadataCatalog


def get_mime_type(obj, default='unknown'):
    obj = IContentTypeAware(obj, obj)
    result = getattr(obj, 'mimeType', None) or getattr(obj, 'mime_type', None)
    return result or default


def find_principal_metadata_objects(principal, accept=(), intids=None):
    intids = component.getUtility(IIntIds) if intids is None else intids
    for obj in get_principal_metadata_objects(principal):
        mime_type = get_mime_type(obj)
        if accept and mime_type not in accept:
            continue
        iid = get_iid(obj, intids=intids)
        if iid is not None:
            yield iid, mime_type, obj


def check_indices(catalog_interface=IMetadataCatalog, intids=None,
                  test_broken=False):
    seen = set()
    broken = dict()
    result = LocatedExternalDict()
    missing = result['Missing'] = set()
    intids = component.getUtility(IIntIds) if intids is None else intids
    catalogs = [c for _, c in component.getUtilitiesFor(catalog_interface)]
    catalogs.append(get_library_catalog())

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

    def _check_btrees(name, index, display=False):
        print("checking", name, index)
        index = getattr(index, 'index', index)
        try:
            import BTrees.check
            for name in ('values_to_documents', 'documents_to_values'):
                item = getattr(index, name, None)
                if item is not None:
                    if hasattr(item, "_check"):
                        item._check()
                    BTrees.check.check(item)
                    if display:
                        BTrees.check.display(item)
        except (ImportError, AttributeError):
            pass

    def _process_catalog(catalog):
        for name, index in catalog.items():
            try:
                if IIndexValues.providedBy(index):
                    _check_btrees(name, index)
                    docids = list(index.ids())
                    processed = _process_ids(catalogs,
                                             docids,
                                             missing,
                                             broken,
                                             seen)
                    if processed:
                        logger.info("%s record(s) unindexed. Source %s,%s",
                                    len(processed), name, catalog)
                elif IKeywordIndex.providedBy(index):
                    _check_btrees(name, index)
                    docids = list(index.ids())
                    processed = _process_ids(catalogs,
                                             docids, 
                                             missing, 
                                             broken, 
                                             seen)
                    if processed:
                        logger.info("%s record(s) unindexed. Source %s,%s",
                                    len(processed), name, catalog)
                elif isinstance(index, TopicIndex):
                    for filter_index in index._filters.values():
                        if ITopicFilteredSet.providedBy(filter_index):
                            _check_btrees(name, filter_index)
                            docids = list(filter_index.getIds())
                            processed = _process_ids(catalogs, 
                                                     docids, 
                                                     missing,
                                                     broken,
                                                     seen)
                            if processed:
                                logger.info("%s record(s) unindexed. Source %s,%s",
                                            len(processed), name, catalog)
            except (POSError, TypeError) as e:
                print(e)
                logger.error('Errors getting ids from index "%s" (%s) in catalog %s',
                             name, index, catalog)

    for catalog in catalogs:
        if catalog is not None:
            _process_catalog(catalog)

    result['Missing'] = sorted(missing)
    result['TotalIndexed'] = len(seen)
    result['TotalMissing'] = len(missing)
    if test_broken:
        result['Broken'] = broken
        result['TotalBroken'] = len(broken)
    return result
