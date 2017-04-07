#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import is_not
from hamcrest import contains
from hamcrest import has_value
from hamcrest import has_entry
from hamcrest import assert_that
from hamcrest import has_entries
from hamcrest import greater_than_or_equal_to

import json

from zope import interface

from ZODB.interfaces import IBroken

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.contenttypes import Note

from nti.externalization.oids import to_external_ntiid_oid

from nti.ntiids.ntiids import make_ntiid

from nti.app.metadata.tests import MetadataApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDSHandleChanges

from nti.appserver.tests.test_application import TestApp

import nti.dataserver.tests.mock_dataserver as mock_dataserver


class TestAdminViews(ApplicationLayerTest):

    layer = MetadataApplicationTestLayer

    def _create_note(self, msg, owner, containerId=None, title=None):
        note = Note()
        if title:
            note.title = IPlainTextContentFragment(title)
        note.body = [unicode(msg)]
        note.creator = owner
        note.containerId = containerId or make_ntiid(
            nttype='bleach',
            specific='manga')
        return note

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
                           json.dumps({'broken': True}),
                           extra_environ=self._make_extra_environ(),
                           status=200)

        assert_that(res.json_body,
                    has_entries('Broken', has_value(u"<class 'nti.dataserver.contenttypes.note.Note'>"),
                                'Missing', is_([]),
                                'TotalBroken', 1,
                                'TotalMissing', 0))

    @WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
    def test_mime_types(self):
        username = 'ichigo@bleach.com'
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_user(username=username)
            note = self._create_note(u'As Nodt Fear', ichigo.username)
            ichigo.addContainedObject(note)

        testapp = TestApp(self.app)
        res = testapp.get('/dataserver2/metadata/mime_types',
                          extra_environ=self._make_extra_environ(),
                          status=200)

        assert_that(res.json_body,
                    has_entries('Items', contains(u"application/vnd.nextthought.note"),
                                'Total', is_(greater_than_or_equal_to(1))))

    @WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
    def test_reindex_user_objects(self):
        username = 'ichigo@bleach.com'
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_user(username=username)
            note = self._create_note(u'As Nodt Fear', ichigo.username)
            ichigo.addContainedObject(note)

        testapp = TestApp(self.app)
        res = testapp.post('/dataserver2/metadata/reindex_user_objects',
                           json.dumps({'all': True,
                                       'system': True}),
                           extra_environ=self._make_extra_environ(),
                           status=200)

        assert_that(res.json_body,
                    has_entries('MimeTypeCount', has_entry('application/vnd.nextthought.note', 1),
                                'Elapsed', is_not(none()),
                                'Total', 1))

    @WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
    def test_reindex(self):
        username = 'ichigo@bleach.com'
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_user(username=username)
            note = self._create_note(u'As Nodt Fear', ichigo.username)
            ichigo.addContainedObject(note)
            ntiid = to_external_ntiid_oid(note)

        testapp = TestApp(self.app)
        res = testapp.post('/dataserver2/metadata/reindex',
                           json.dumps({'all': True,
                                       'ntiid': ntiid}),
                           extra_environ=self._make_extra_environ(),
                           status=200)

        assert_that(res.json_body,
                    has_entries('Total', 1))
