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

import zope.intid

from zope import component

from ZODB.POSException import POSError

from nti.dataserver.interfaces import IUser
from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout

from nti.dataserver.users import User

from nti.externalization.interfaces import LocatedExternalDict

from nti.metadata import metadata_queue
from nti.metadata.reactor import process_queue
from nti.metadata import get_principal_metadata_objects_intids

def get_mimeType(obj):
	result = getattr(obj, 'mimeType', None) or getattr(obj, 'mime_type', None)
	return result or 'unknown'

def reindex(usernames=(), accept=(), queue_limit=None, intids=None):
	total = 0
	if usernames:
		dataserver = component.getUtility(IDataserver)
		users_folder = IShardLayout(dataserver).users_folder
		usernames = users_folder.keys()
	
	now = time.time()
	queue = metadata_queue()
	mimeType_count = defaultdict(int)
	intids = component.getUtility(zope.intid.IIntIds) if intids is None else intids
	
	for username in usernames:
		user = User.get_user(username)
		if user is None or not IUser.providedBy(user):
			continue
		for iid in get_principal_metadata_objects_intids(user):
			try:
				obj = intids.queryObject(iid)
				mimeType = get_mimeType(obj)
				if accept and mimeType not in accept:
					continue
				queue.add(iid)
			except TypeError:
				pass
			except POSError:
				logger.error("ignoring broken object %s", iid)
			else:
				total += 1
				mimeType_count[mimeType] = mimeType_count[mimeType] + 1 

	if queue_limit is not None:
		process_queue(limit=queue_limit)
		
	elapsed = time.time() - now
	result = LocatedExternalDict()
	result['Total'] = total
	result['Elapsed'] = elapsed
	result['MimeTypeCount'] = dict(mimeType_count)
	
	logger.info("%s object(s) processed in %s(s)", total, elapsed)
	return result

# script methods

import os
import pprint
import argparse

import zope.browserpage

from zope.container.contained import Contained
from zope.configuration import xmlconfig, config
from zope.dottedname import resolve as dottedname

from z3c.autoinclude.zcml import includePluginsDirective

from nti.dataserver.utils import run_with_dataserver

class PluginPoint(Contained):

	def __init__(self, name):
		self.__name__ = name

PP_APP = PluginPoint('nti.app')
PP_APP_SITES = PluginPoint('nti.app.sites')
PP_APP_PRODUCTS = PluginPoint('nti.app.products')

def _create_context(env_dir, devmode=False):
	etc = os.getenv('DATASERVER_ETC_DIR') or os.path.join(env_dir, 'etc')
	etc = os.path.expanduser(etc)

	context = config.ConfigurationMachine()
	xmlconfig.registerCommonDirectives(context)

	if devmode:
		context.provideFeature("devmode")
		
	slugs = os.path.join(etc, 'package-includes')
	if os.path.exists(slugs) and os.path.isdir(slugs):
		package = dottedname.resolve('nti.dataserver')
		context = xmlconfig.file('configure.zcml', package=package, context=context)
		xmlconfig.include(context, files=os.path.join(slugs, '*.zcml'),
						  package='nti.appserver')

	library_zcml = os.path.join(etc, 'library.zcml')
	if not os.path.exists(library_zcml):
		raise Exception("could not locate library zcml file %s", library_zcml)
	xmlconfig.include(context, file=library_zcml)
	
	# Include zope.browserpage.meta.zcm for tales:expressiontype
	# before including the products
	xmlconfig.include(context, file="meta.zcml", package=zope.browserpage)

	# include plugins
	includePluginsDirective(context, PP_APP)
	includePluginsDirective(context, PP_APP_SITES)
	includePluginsDirective(context, PP_APP_PRODUCTS)
	
	return context

def _process_args(args):
	result = reindex(queue_limit=args.limit,
					 accept=args.types or (),
					 usernames=args.usernames or ())
		
	if args.verbose:
		pprint.pprint(result)
	return result
	
def main():
	arg_parser = argparse.ArgumentParser(description="Hypatia content reindexer")
	arg_parser.add_argument('-v', '--verbose', help="Be verbose", action='store_true',
							 dest='verbose')
	arg_parser.add_argument('-t', '--types',
							dest='types',
							nargs="+",
							help="The object mime types to index")
	arg_parser.add_argument('-u', '--usernames',
							dest='usernames',
							nargs="+",
							help="The object creator user names")
	arg_parser.add_argument('-l', '--limit',
							 dest='limit',
							 help="Queue limit",
							 type=int)

	args = arg_parser.parse_args()
	env_dir = os.getenv('DATASERVER_DIR')
	if not env_dir or not os.path.exists(env_dir) and not os.path.isdir(env_dir):
		raise IOError("Invalid dataserver environment root directory")

	context = _create_context(env_dir)
	conf_packages = ('nti.appserver', 'nti.app.hypatia')

	run_with_dataserver(environment_dir=env_dir,
						xmlconfig_packages=conf_packages,
						verbose=args.verbose,
						context=context,
						minimal_ds=True,
						function=lambda: _process_args(args))

if __name__ == '__main__':
	main()
