#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time
from collections import defaultdict

from zope import component

from zope.intid import IIntIds

from zope.security.management import system_user

from nti.dataserver.interfaces import IUser
from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout

from nti.dataserver.users import User

from nti.externalization.interfaces import LocatedExternalDict

from nti.metadata import get_iid
from nti.metadata import metadata_queue
from nti.metadata.reactor import process_queue

from . import find_principal_metadata_objects

def reindex_principal(principal, accept=(), queue=None, intids=None, mt_count=None):
	result = 0
	queue = metadata_queue() if queue is None else queue
	mt_count = defaultdict(int) if mt_count is None else mt_count
	intids = component.getUtility(IIntIds) if intids is None else intids
	for iid, mimeType, obj in find_principal_metadata_objects(principal, accept, intids):
		try:
			iid = get_iid(obj, intids=intids)
			if iid is not None:
				queue.add(iid)
		except TypeError:
			pass
		else:
			result += 1
			mt_count[mimeType] = mt_count[mimeType] + 1
	return result, mt_count

def reindex(usernames=(), all_users=False, system=False, accept=(),
			queue_limit=None, intids=None):
	if all_users:
		dataserver = component.getUtility(IDataserver)
		users_folder = IShardLayout(dataserver).users_folder
		usernames = users_folder.keys()

	total = 0
	now = time.time()
	queue = metadata_queue()
	mt_count = defaultdict(int)
	intids = component.getUtility(IIntIds) if intids is None else intids

	for username in usernames or ():
		user = User.get_user(username)
		if user is None or not IUser.providedBy(user):
			continue
		count, _ = reindex_principal(user, accept, queue=queue, intids=intids,
									 mt_count=mt_count)
		total += count

	if system:
		count, _ = reindex_principal(system_user(), accept, queue=queue, intids=intids,
									 mt_count=mt_count)
		total += count

	if queue_limit is not None:
		process_queue(limit=queue_limit)

	elapsed = time.time() - now
	result = LocatedExternalDict()
	result['Total'] = total
	result['Elapsed'] = elapsed
	result['MimeTypeCount'] = dict(mt_count)

	logger.info("%s object(s) processed in %s(s)", total, elapsed)
	return result
