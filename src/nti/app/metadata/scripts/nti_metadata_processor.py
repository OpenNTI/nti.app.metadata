#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import interface

from zope.location.interfaces import ILocation

from z3c.autoinclude.zcml import includePluginsDirective

from nti.app.asynchronous.processor import Processor

from nti.dataserver.utils.base_script import create_context

from nti.metadata import QUEUE_NAMES

logger = __import__('logging').getLogger(__name__)


@interface.implementer(ILocation)
class PluginPoint(object):
    __parent__ = None

    def __init__(self, name):
        self.__name__ = name
PP_METADATA = PluginPoint('nti.metadata')


class Constructor(Processor):

    def extend_context(self, context):
        includePluginsDirective(context, PP_METADATA)

    def create_context(self, env_dir, args):
        slugs = args.slugs
        context = create_context(env_dir,
                                 slugs=slugs,
                                 plugins=slugs,
                                 with_library=True)
        self.extend_context(context)
        return context

    def process_args(self, args):
        setattr(args, 'redis', True)
        setattr(args, 'library', True)
        setattr(args, 'trx_retries', 9)
        setattr(args, 'max_sleep_time', 30)
        setattr(args, 'queue_names', QUEUE_NAMES)
        super(Constructor, self).process_args(args)


def main():
    return Constructor()()


if __name__ == '__main__':
    main()
