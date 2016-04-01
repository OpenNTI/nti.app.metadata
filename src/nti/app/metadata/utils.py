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

def check_indices(catalog_interface=IMetadataCatalog, intids=None):
	seen = set()
	result = LocatedExternalDict()
	broken = result['Broken'] = {}
	missing = result['Missing'] = set()	
	intids = component.getUtility(IIntIds) if intids is None else intids

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
				elif isBroken(obj):
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

	catalogs = [catalog for _, catalog in component.getUtilitiesFor(catalog_interface)]
	for catalog in catalogs:
		for name, index in catalog.items():
			try:
				if IIndexValues.providedBy(index):
					docids = list(index.ids())
					processed = _process_ids(catalogs, docids, missing, broken, seen)
					if processed:
						logger.info("%s record(s) unindexed. Source %s,%s", 
									len(processed), name, catalog)
				elif IKeywordIndex.providedBy(index):
					docids = list(index.ids())
					processed = _process_ids(catalogs, docids, missing, broken, seen)
					if processed:
						logger.info("%s record(s) unindexed. Source %s,%s", 
									len(processed), name, catalog)
				elif isinstance(index, TopicIndex):
					for filter_index in index._filters.values():
						if isinstance(filter_index, ITopicFilteredSet):
							docids = list(filter_index.getIds())
							processed = _process_ids(catalogs, docids, missing, 
													 broken, seen)
							if processed:
								logger.info("%s record(s) unindexed. Source %s,%s",
											len(processed), name, catalog)
			except (POSError, TypeError):
				logger.error('Errors getting ids from index "%s" (%s) in catalog %s', 
							 name, index, catalog)

	result['Missing'] = list(missing)
	result['TotalIndexed'] = len(seen)
	result['TotalBroken'] = len(broken)
	result['TotalMissing'] = len(missing)
	return result
