#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import os
import pprint
import argparse

from zope.catalog.interfaces import ICatalog

from nti.app.metadata.utils import check_indices

from nti.dataserver.utils import run_with_dataserver
from nti.dataserver.utils.base_script import create_context

from nti.zope_catalog.interfaces import IMetadataCatalog


def _process_args(args):
    if args.all:
        catalog_interface = ICatalog
    else:
        catalog_interface = IMetadataCatalog

    result = check_indices(catalog_interface=catalog_interface,
                           test_broken=args.broken,
                           inspect_btrees=args.check)
    if args.verbose:
        pprint.pprint(result)
    return result


def main():
    arg_parser = argparse.ArgumentParser(description="Metadata index checker")
    arg_parser.add_argument('-v', '--verbose', help="Be verbose", 
                            action='store_true',
                            dest='verbose')
    arg_parser.add_argument('-a', '--all', help="Include all catalogs",
                            action='store_true',
                            dest='all')
    arg_parser.add_argument('-c', '--check', help="Check BTrees",
                            action='store_true',
                            dest='check')
    arg_parser.add_argument('-b', '--broken', help="Test for broken objects",
                            action='store_true',
                            dest='broken')

    args = arg_parser.parse_args()
    env_dir = os.getenv('DATASERVER_DIR')
    if not env_dir or not os.path.exists(env_dir) and not os.path.isdir(env_dir):
        raise IOError("Invalid dataserver environment root directory")

    context = create_context(env_dir, True)
    conf_packages = ('nti.appserver', 'nti.app.metadata')

    run_with_dataserver(environment_dir=env_dir,
                        xmlconfig_packages=conf_packages,
                        verbose=args.verbose,
                        context=context,
                        minimal_ds=True,
                        function=lambda: _process_args(args))

if __name__ == '__main__':
    main()
