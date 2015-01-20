#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import is_not
from hamcrest import has_value
from hamcrest import has_entry
from hamcrest import assert_that
from hamcrest import has_entries

import json

from zope import interface

from ZODB.interfaces import IBroken

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.contenttypes import Note

from nti.ntiids.ntiids import make_ntiid

from nti.appserver.tests.test_application import TestApp

import nti.dataserver.tests.mock_dataserver as mock_dataserver

from nti.app.metadata.tests import MetadataApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest
from nti.app.testing.decorators import WithSharedApplicationMockDSHandleChanges

class TestAdminViews(ApplicationLayerTest):

	layer = MetadataApplicationTestLayer

	def _create_note(self, msg, owner, containerId=None, title=None):
		note = Note()
		if title:
			note.title = IPlainTextContentFragment(title)
		note.body = [unicode(msg)]
		note.creator = owner
		note.containerId = containerId or make_ntiid(nttype='bleach', specific='manga')
		return note

	@WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
	def test_process_queue(self):
		username = 'ichigo@bleach.com'
		with mock_dataserver.mock_db_trans(self.ds):
			ichigo = self._create_user(username=username)
			note = self._create_note(u'As Nodt Fear', ichigo.username)
			ichigo.addContainedObject(note)

		testapp = TestApp(self.app)
		testapp.post('/dataserver2/metadata/process_queue',
					 extra_environ=self._make_extra_environ(),
					 status=200)
			
		testapp.post('/dataserver2/metadata/process_queue',
					 json.dumps({'limit': 'xyt'}),
					 extra_environ=self._make_extra_environ(),
					 status=422)

	@WithSharedApplicationMockDSHandleChanges(testapp=False, users=True)
	def test_sync_queue(self):
		with mock_dataserver.mock_db_trans(self.ds):
			for x in range(10):
				usr = self._create_user(username='bankai%s' % x)
				note = self._create_note(u'Shikai %s' % x, usr.username)
				usr.addContainedObject(note)

		testapp = TestApp(self.app)
		testapp.post('/dataserver2/metadata/sync_queue',
					 extra_environ=self._make_extra_environ(),
					 status=204)
		
	@WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
	def test_check_indices(self):
		username = 'ichigo@bleach.com'
		with mock_dataserver.mock_db_trans(self.ds):
			ichigo = self._create_user(username=username)
			note = self._create_note(u'As Nodt Fear', ichigo.username)
			ichigo.addContainedObject(note)
			
			note = self._create_note(u'Broken', ichigo.username)
			ichigo.addContainedObject(note)
			interface.alsoProvides(note, IBroken)

		testapp = TestApp(self.app)
		res = testapp.post('/dataserver2/metadata/check_indices',
					 		extra_environ=self._make_extra_environ(),
					 		status=200)
		
		assert_that(res.json_body, 
					has_entries('Broken', has_value(u"<class 'nti.dataserver.contenttypes.note.Note'>"),
								'Missing', is_([]), 
								'TotalBroken', 1,
								'TotalMissing', 0) )
		
	@WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
	def test_reindex(self):
		username = 'ichigo@bleach.com'
		with mock_dataserver.mock_db_trans(self.ds):
			ichigo = self._create_user(username=username)
			note = self._create_note(u'As Nodt Fear', ichigo.username)
			ichigo.addContainedObject(note)

		testapp = TestApp(self.app)
		res = testapp.post('/dataserver2/metadata/reindex',
							json.dumps({'all': True,
										'system':True}),
					 		extra_environ=self._make_extra_environ(),
					 		status=200)
		
		assert_that(res.json_body, 
					has_entries('MimeTypeCount', has_entry('application/vnd.nextthought.note', 1),
								'Elapsed', is_not(none()), 
								'Total', 1) )
