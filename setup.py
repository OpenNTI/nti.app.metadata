import codecs
from setuptools import setup, find_packages

VERSION = '0.0.0'

entry_points = {
    "z3c.autoinclude.plugin": [
		'target = nti.app',
	],
    "console_scripts": [
        "nti_metadata_reindexer = nti.app.metadata.scripts.reindexer:main"
    ],
}

setup(
    name='nti.app.metadata',
    version=VERSION,
    author='Jason Madden',
    author_email='jason@nextthought.com',
    description="NTI Metadata App",
    long_description=codecs.open('README.rst', encoding='utf-8').read(),
    license='Proprietary',
    keywords='pyramid preference',
    classifiers=[
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
    ],
    packages=find_packages('src'),
    package_dir={'': 'src'},
    namespace_packages=['nti','nti.app'],
    install_requires=[
        'setuptools',
        'nti.metadata'
    ],
    entry_points=entry_points
)
