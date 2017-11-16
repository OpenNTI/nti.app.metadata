#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import pprint
import argparse

from zope import component

from nti.app.metadata.reindexer import reindex

from nti.dataserver.utils import run_with_dataserver
from nti.dataserver.utils.base_script import set_site
from nti.dataserver.utils.base_script import create_context

logger = __import__('logging').getLogger(__name__)


def _load_library():
    try:
        from nti.contentlibrary.interfaces import IContentPackageLibrary
        library = component.queryUtility(IContentPackageLibrary)
        if library is not None:
            library.syncContentPackages()
    except ImportError:
        pass


def _process_args(args):
    _load_library()
    set_site(args.site)
    result = reindex(system=args.system,
                     accept=args.types or (),
                     usernames=args.usernames or ())
    if args.verbose:
        pprint.pprint(result)
    return result


def main():
    arg_parser = argparse.ArgumentParser(description="Metadata object reindexer")
    arg_parser.add_argument('-v', '--verbose', help="Be verbose", action='store_true',
                            dest='verbose')
    arg_parser.add_argument('-t', '--types',
                            dest='types',
                            nargs="+",
                            help="The mime types")
    arg_parser.add_argument('-s', '--site', help="Application site",
                            dest='site')
    arg_parser.add_argument('-m', '--system', help="Include system user",
                            action='store_true',
                            dest='system')
    site_group = arg_parser.add_mutually_exclusive_group()
    site_group.add_argument('-u', '--usernames',
                            dest='usernames',
                            nargs="+",
                            help="The user names")

    args = arg_parser.parse_args()
    env_dir = os.getenv('DATASERVER_DIR')
    if not env_dir or not os.path.exists(env_dir) and not os.path.isdir(env_dir):
        raise IOError("Invalid dataserver environment root directory")

    context = create_context(env_dir, True)
    conf_packages = ('nti.appserver',)

    run_with_dataserver(environment_dir=env_dir,
                        xmlconfig_packages=conf_packages,
                        verbose=args.verbose,
                        context=context,
                        minimal_ds=True,
                        function=lambda: _process_args(args))


if __name__ == '__main__':
    main()
