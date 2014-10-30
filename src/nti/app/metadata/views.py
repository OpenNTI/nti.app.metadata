#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""
from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time

import zope.intid

from zope import component
from zope import interface
from zope.container.contained import Contained
from zope.traversing.interfaces import IPathAdapter

from pyramid.view import view_config
from pyramid import httpexceptions as hexc

from nti.app.base.abstract_views import AbstractAuthenticatedView
from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.dataserver import authorization as nauth

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.externalization import to_external_object
from nti.externalization.externalization import NonExternalizableObjectError

from nti.metadata import metadata_queue
from nti.metadata.reactor import process_queue
from nti.metadata.interfaces import DEFAULT_QUEUE_LIMIT

from nti.utils.maps import CaseInsensitiveDict

@interface.implementer(IPathAdapter)
class MetadataPathAdapter(Contained):

	__name__ = 'metadata'

	def __init__(self, context, request):
		self.context = context
		self.request = request
		self.__parent__ = context

@view_config(route_name='objects.generic.traversal',
			 name='process_queue',
			 renderer='rest',
			 request_method='POST',
			 context=MetadataPathAdapter,
			 permission=nauth.ACT_MODERATE)
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
		total = process_queue(limit)
		result = LocatedExternalDict()
		result['Elapsed'] = time.time() - now
		result['Total'] = total
		return result

@view_config(route_name='objects.generic.traversal',
			 name='queued_objects',
			 renderer='rest',
			 request_method='GET',
			 context=MetadataPathAdapter,
			 permission=nauth.ACT_MODERATE)
class QueuedObjectsView(AbstractAuthenticatedView):
	
	def __call__(self):
		intids = component.getUtility(zope.intid.IIntIds)
		catalog_queue = metadata_queue()
		result = LocatedExternalDict()
		items = result['Items'] = {}
		for key in catalog_queue.keys():
			try:
				obj = intids.queryObject(key)
				if obj is not None:
					items[key] = to_external_object(obj)
			except NonExternalizableObjectError:
				items[key] = { 	'Object': str(type(obj)) }
			except Exception as e:
				items[key] = {	'Message': str(e),
								'Object': str(type(obj)),
								'Exception': str(type(e))}
			
		result['Total'] = len(items)
		return result
	
@view_config(route_name='objects.generic.traversal',
			 name='sync_queue',
			 renderer='rest',
			 request_method='POST',
			 context=MetadataPathAdapter,
			 permission=nauth.ACT_MODERATE)
class SyncQueueView(AbstractAuthenticatedView, 
					ModeledContentUploadRequestUtilsMixin):
	
	def __call__(self):
		catalog_queue = metadata_queue()
		if catalog_queue.syncQueue():
			logger.info("Queue synched")
		return hexc.HTTPNoContent()

