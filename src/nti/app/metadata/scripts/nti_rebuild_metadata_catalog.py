#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import sys
import pprint
import argparse

from zope import component

from nti.app.metadata.reindexer import rebuild_metadata_catalog

from nti.dataserver.utils import run_with_dataserver

from nti.dataserver.utils.base_script import create_context

from nti.externalization.interfaces import StandardExternalFields

ITEMS = StandardExternalFields.ITEMS
TOTAL = StandardExternalFields.TOTAL
ITEM_COUNT = StandardExternalFields.ITEM_COUNT

logger = __import__('logging').getLogger(__name__)


def _sync_library():
    try:
        from nti.contentlibrary.interfaces import IContentPackageLibrary
        library = component.getUtility(IContentPackageLibrary)
        if library is not None:
            library.syncContentPackages()
    except ImportError:
        pass


def _process_args(args):
    _sync_library()
    count = rebuild_metadata_catalog()
    if args.verbose:
        result = {
            TOTAL: count,
            ITEM_COUNT: count
        }
        pprint.pprint(result)


def main():
    arg_parser = argparse.ArgumentParser(description="Rebuild metadata catalog")
    arg_parser.add_argument('-v', '--verbose', help="Be Verbose", action='store_true',
                            dest='verbose')

    args = arg_parser.parse_args()
    env_dir = os.getenv('DATASERVER_DIR')
    if not env_dir or not os.path.exists(env_dir) and not os.path.isdir(env_dir):
        raise IOError("Invalid dataserver environment root directory")

    conf_packages = ('nti.appserver',)
    context = create_context(env_dir, with_library=True)

    run_with_dataserver(environment_dir=env_dir,
                        verbose=args.verbose,
                        xmlconfig_packages=conf_packages,
                        context=context,
                        minimal_ds=True,
                        function=lambda: _process_args(args))
    sys.exit(0)


if __name__ == '__main__':
    main()
