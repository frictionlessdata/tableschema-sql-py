from setuptools import setup, find_packages

try:
    from jtssql import __version__
except:
    __version__ = 'dev'

setup(
    name='jtssql',
    version=__version__,
    description="Generate database tables based on JSON Table Schema",
    long_description="",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4'
    ],
    keywords='schema jsontableschema jts sql data',
    author='Friedrich Lindenberg',
    author_email='friedrich@pudo.org',
    url='http://github.com/okfn/jtssql',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'test']),
    namespace_packages=[],
    package_data={},
    include_package_data=True,
    zip_safe=False,
    test_suite='nose.collector',
    install_requires=[
        'sqlalchemy >= 1.0',
        'typecast >= 0.2.2',
        'normality'
    ],
    tests_require=[
        'nose',
        'coverage',
        'wheel',
        'unicodecsv',
        'jtskit'
    ]
)
