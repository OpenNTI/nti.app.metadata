#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import is_in
from hamcrest import is_not
from hamcrest import has_value
from hamcrest import has_entry
from hamcrest import assert_that
from hamcrest import has_entries
from hamcrest import greater_than_or_equal_to

import simplejson as json

from zope import component
from zope import interface

from zope.intid.interfaces import IIntIds

from ZODB.interfaces import IBroken

from nti.app.metadata.tests import MetadataApplicationTestLayer

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDSHandleChanges

from nti.appserver.tests.test_application import TestApp

from nti.base._compat import text_

from nti.contentfragments.interfaces import IPlainTextContentFragment

from nti.dataserver.contenttypes.note import Note

from nti.dataserver.metadata.index import get_metadata_catalog

from nti.dataserver.tests import mock_dataserver

from nti.ntiids.ntiids import make_ntiid


class TestAdminViews(ApplicationLayerTest):

    layer = MetadataApplicationTestLayer

    def _create_note(self, msg, owner, containerId=None, title=None):
        note = Note()
        if title:
            note.title = IPlainTextContentFragment(title)
        note.body = [text_(msg)]
        note.creator = owner
        note.containerId = containerId \
                        or make_ntiid(nttype=u'bleach', specific=u'manga')
        return note

    @WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
    def test_check_indices(self):
        username = u'ichigo@bleach.com'
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_user(username=username)
            note = self._create_note(u'As Nodt Fear', ichigo.username)
            ichigo.addContainedObject(note)

            note = self._create_note(u'Broken', ichigo.username)
            ichigo.addContainedObject(note)
            interface.alsoProvides(note, IBroken)

        testapp = TestApp(self.app)
        res = testapp.post('/dataserver2/metadata/@@check_indices',
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
        username = u'ichigo@bleach.com'
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_user(username=username)
            note = self._create_note(u'As Nodt Fear', ichigo.username)
            ichigo.addContainedObject(note)

        testapp = TestApp(self.app)
        res = testapp.get('/dataserver2/metadata/@@mime_types',
                          extra_environ=self._make_extra_environ(),
                          status=200)
        assert_that(res.json_body,
                    has_entries('Total', is_(greater_than_or_equal_to(3))))

    @WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
    def test_unindex_index_doc(self):
        username = u'ichigo@bleach.com'
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_user(username=username)
            note = self._create_note(u'As Nodt Fear', ichigo.username)
            ichigo.addContainedObject(note)
            intids = component.getUtility(IIntIds)
            doc_id = intids.queryId(note)

        testapp = TestApp(self.app)
        testapp.post('/dataserver2/metadata/unindex_doc/%s' % doc_id,
                     extra_environ=self._make_extra_environ(),
                     status=204)

        with mock_dataserver.mock_db_trans(self.ds):
            catalog = get_metadata_catalog()
            index = catalog['mimeType']
            assert_that(doc_id, is_not(is_in(index.ids())))

        testapp.post('/dataserver2/metadata/index_doc/%s' % doc_id,
                     extra_environ=self._make_extra_environ(),
                     status=204)

        with mock_dataserver.mock_db_trans(self.ds):
            catalog = get_metadata_catalog()
            index = catalog['mimeType']
            assert_that(doc_id, is_in(index.ids()))

    @WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
    def test_reindexer(self):
        username = u'ichigo@bleach.com'
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_user(username=username)
            note = self._create_note(u'As Nodt Fear', ichigo.username)
            ichigo.addContainedObject(note)

        testapp = TestApp(self.app)
        res = testapp.post('/dataserver2/metadata/reindexer',
                           json.dumps({'username': username,
                                       'system': False}),
                           extra_environ=self._make_extra_environ(),
                           status=200)

        assert_that(res.json_body,
                    has_entries('MimeTypeCount', has_entry('application/vnd.nextthought.note', 1),
                                'Elapsed', is_not(none()),
                                'Total', greater_than_or_equal_to(1)))

    @WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
    def test_usg(self):
        username = u'ichigo@bleach.com'
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_user(username=username)
            note = self._create_note(u'Kurosaki Ichigo', ichigo.username)
            ichigo.addContainedObject(note)
            ntiid = note.containerId

        testapp = TestApp(self.app)
        res = testapp.get('/dataserver2/metadata/@@UserUGD',
                          {
                              'username': username,
                              'ntiid': ntiid
                          },
                          extra_environ=self._make_extra_environ(),
                          status=200)

        assert_that(res.json_body,
                    has_entries('Total', 1))

    @WithSharedApplicationMockDSHandleChanges(users=True, testapp=True)
    def test_rebuild_catalog(self):
        username = u'ichigo@bleach.com'
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_user(username=username)
            note = self._create_note(u'Kurosaki Ichigo', ichigo.username)
            ichigo.addContainedObject(note)

        testapp = TestApp(self.app)
        res = testapp.post('/dataserver2/metadata/@@RebuildMetadataCatalog',
                           extra_environ=self._make_extra_environ(),
                           status=200)
        assert_that(res.json_body,
                    has_entries('Total', greater_than_or_equal_to(2)))
