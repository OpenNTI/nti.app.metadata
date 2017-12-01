import codecs
from setuptools import setup, find_packages

entry_points = {
    "z3c.autoinclude.plugin": [
        'target = nti.app',
    ],
    "console_scripts": [
        "nti_check_indices = nti.app.metadata.scripts.nti_check_indices:main",
        "nti_metadata_processor = nti.app.metadata.scripts.nti_metadata_processor:main",
        "nti_metadata_reindexer = nti.app.metadata.scripts.nti_metadata_reindexer:main",
        "nti_rebuild_metadata_catalog = nti.app.metadata.scripts.nti_rebuild_metadata_catalog:main",
    ],
}

TESTS_REQUIRE = [
    'nti.app.testing',
    'nti.testing',
    'zope.dottedname',
    'zope.testrunner',
]


def _read(fname):
    with codecs.open(fname, encoding='utf-8') as f:
        return f.read()


setup(
    name='nti.app.metadata',
    version=_read('version.txt').strip(),
    author='Jason Madden',
    author_email='jason@nextthought.com',
    description="The Metadata app layer",
    long_description=(_read('README.rst') + '\n\n' + _read('CHANGES.rst')),
    license='Apache',
    keywords='pyramid metadata',
    classifiers=[
        'Framework :: Zope',
        'Framework :: Pyramid',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    url="https://github.com/NextThought/nti.app.metadata",
    zip_safe=True,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    include_package_data=True,
    namespace_packages=['nti', 'nti.app'],
    tests_require=TESTS_REQUIRE,
    install_requires=[
        'setuptools',
        'BTrees',
        'nti.app.asynchronous',
        'nti.async',
        'nti.base',
        'nti.common',
        'nti.contentfragments',
        'nti.externalization',
        'nti.metadata',
        'nti.ntiids',
        'nti.zope_catalog',
        'pyramid',
        'requests',
        'six',
        'z3c.autoinclude',
        'zc.catalog',
        'ZODB',
        'zope.cachedescriptors',
        'zope.catalog',
        'zope.component',
        'zope.generations',
        'zope.index',
        'zope.intid',
        'zope.location',
        'zope.mimetype',
        'zope.security',
        'zope.traversing',
    ],
    extras_require={
        'test': TESTS_REQUIRE,
        'docs': [
            'Sphinx',
            'repoze.sphinx.autointerface',
            'sphinx_rtd_theme',
        ],
    },
    entry_points=entry_points
)
