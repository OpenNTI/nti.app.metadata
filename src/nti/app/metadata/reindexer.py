#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import time
from collections import defaultdict

from zc.catalog.interfaces import IIndexValues

from zc.catalog.index import NormalizationWrapper

from ZODB.POSException import POSError

from zope import component

from zope.index.topic import TopicIndex

from zope.index.topic.interfaces import ITopicFilteredSet

from zope.intid.interfaces import IIntIds

from zope.security.management import system_user

from nti.app.metadata.utils import principal_metadata_objects

from nti.dataserver.interfaces import IUser

from nti.dataserver.metadata.index import add_catalog_filters
from nti.dataserver.metadata.index import get_metadata_catalog

from nti.dataserver.users.users import User

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.metadata import queue_add

from nti.zope_catalog.interfaces import IKeywordIndex

TOTAL = StandardExternalFields.TOTAL
ITEM_COUNT = StandardExternalFields.ITEM_COUNT

logger = __import__('logging').getLogger(__name__)


def reindex_principal(principal, accept=(), intids=None, mt_count=None, seen=None):
    result = 0
    seen = set() if seen is None else seen
    mt_count = defaultdict(int) if mt_count is None else mt_count
    intids = component.getUtility(IIntIds) if intids is None else intids
    for iid, mimeType, _ in principal_metadata_objects(principal, accept, intids):
        if iid in seen:
            continue
        result += 1
        seen.add(iid)
        queue_add(iid)
        mt_count[mimeType] = mt_count[mimeType] + 1
    return result


def reindex(usernames=(), system=False, accept=(), intids=None):
    total = 0
    seen = set()
    now = time.time()
    mt_count = defaultdict(int)
    intids = component.getUtility(IIntIds) if intids is None else intids
    for username in usernames or ():
        user = User.get_user(username)
        if not IUser.providedBy(user):
            continue
        total += reindex_principal(user,
                                   accept,
                                   seen=seen,
                                   intids=intids,
                                   mt_count=mt_count)

    if system:
        total += reindex_principal(system_user(),
                                   accept,
                                   seen=seen,
                                   intids=intids,
                                   mt_count=mt_count)

    elapsed = time.time() - now
    result = LocatedExternalDict()
    result[TOTAL] = total
    result['Elapsed'] = elapsed
    result['MimeTypeCount'] = dict(mt_count)
    logger.info("%s object(s) processed in %s(s)", total, elapsed)
    return result


def get_catalog_doc_ids(catalog):
    seen = set()
    for name, index in catalog.items():
        if isinstance(index, NormalizationWrapper):
            index = index.index
        try:
            if IIndexValues.providedBy(index):
                seen.update(index.ids())
            elif IKeywordIndex.providedBy(index):
                seen.update(index.ids())
            elif isinstance(index, TopicIndex):
                # pylint: disable=protected-access
                for filter_index in index._filters.values():
                    if ITopicFilteredSet.providedBy(filter_index):
                        seen.update(filter_index.getIds())
        except (POSError, TypeError) as e:
            logger.error('Error %s while getting ids from index "%s" (%s)',
                         e, name, index)
    return seen


def rebuild_metadata_catalog(seen=None):
    intids = component.getUtility(IIntIds)
    # get all ids and clear indexes
    catalog = get_metadata_catalog()
    doc_ids = get_catalog_doc_ids(catalog)
    for index in catalog.values():
        index.clear()
    # filters need to be added
    add_catalog_filters(catalog, catalog.family)
    # reindex
    count = 0
    seen = set() if seen is None else seen
    logger.info("Processing %s object(s)", len(doc_ids))
    for doc_id in doc_ids:
        obj = intids.queryObject(doc_id)
        if obj is None:
            logger.debug("%s is missing", doc_id)
            continue
        try:
            catalog.force_index_doc(doc_id, obj)
        except (POSError, TypeError) as e:
            logger.error('Error %s while indexing %s, %s',
                         e, doc_id, type(obj))
            try:
                intids.force_unregister(doc_id)
            except (AttributeError, KeyError):
                pass
        else:
            count += 1
            seen.add(doc_id)
    logger.info("%s object(s) indexed", count)
    return count
