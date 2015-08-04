#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component

from zope.intid import IIntIds

from zope.mimetype.interfaces import IContentTypeAware

from nti.metadata import get_iid
from nti.metadata import get_principal_metadata_objects

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
