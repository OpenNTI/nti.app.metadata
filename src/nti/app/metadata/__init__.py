#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import zope.intid

from zope import component

from zope.mimetype.interfaces import IContentTypeAware

from ZODB.POSException import POSError

from nti.metadata import get_iid
from nti.metadata import get_principal_metadata_objects

def get_mime_type(obj, default='unknown'):
    obj = IContentTypeAware(obj, obj)
    result = getattr(obj, 'mimeType', None) or getattr(obj, 'mime_type', None)
    return result or default

def find_principal_metadata_objects(principal, accept=(), intids=None):
    intids = component.getUtility(zope.intid.IIntIds) if intids is None else intids
    for obj in get_principal_metadata_objects(principal):
        try:
            mime_type = get_mime_type(obj)
            if accept and mime_type not in accept:
                continue
            iid = get_iid(obj, intids=intids)
            if iid is not None:
                yield iid, mime_type, obj
        except (TypeError, POSError):
            logger.error("ignoring broken object %s", type(obj))
