#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time
from collections import defaultdict

from zope import component

from zope.intid.interfaces import IIntIds

from zope.security.management import system_user

from nti.app.metadata.utils import find_principal_metadata_objects

from nti.dataserver.interfaces import IUser

from nti.dataserver.users.users import User

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.metadata import get_iid
from nti.metadata import queue_add

TOTAL = StandardExternalFields.TOTAL


def reindex_principal(principal, accept=(), intids=None, mt_count=None):
    result = 0
    mt_count = defaultdict(int) if mt_count is None else mt_count
    intids = component.getUtility(IIntIds) if intids is None else intids
    for iid, mimeType, obj in find_principal_metadata_objects(principal, accept, intids):
        iid = get_iid(obj, intids=intids)
        if iid is not None:
            result += 1
            queue_add(iid)
            mt_count[mimeType] = mt_count[mimeType] + 1
    return result


def reindex(usernames=(), system=False, accept=(), intids=None):
    total = 0
    now = time.time()
    mt_count = defaultdict(int)
    intids = component.getUtility(IIntIds) if intids is None else intids
    for username in usernames or ():
        user = User.get_user(username)
        if user is None or not IUser.providedBy(user):
            continue
        total += reindex_principal(user,
                                   accept,
                                   intids=intids,
                                   mt_count=mt_count)

    if system:
        total += reindex_principal(system_user(),
                                   accept,
                                   intids=intids,
                                   mt_count=mt_count)

    elapsed = time.time() - now
    result = LocatedExternalDict()
    result[TOTAL] = total
    result['Elapsed'] = elapsed
    result['MimeTypeCount'] = dict(mt_count)
    logger.info("%s object(s) processed in %s(s)", total, elapsed)
    return result
