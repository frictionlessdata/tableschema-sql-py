# -*- coding: utf-8 -*-
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import
from __future__ import unicode_literals

import os
import io
import json
from setuptools import setup, find_packages


# Helpers
def read(path):
    basedir = os.path.dirname(__file__)
    return io.open(os.path.join(basedir, path), encoding='utf-8').read()
def clean(deps):
    res = []
    for dep in deps:
        if dep and not dep.startswith('#') and not dep.startswith('git'):
            res.append(dep)
    return res


# Prepare
readme = read('README.md')
license = read('LICENSE.txt')
requirements = clean(read('requirements.txt').split())
requirements_dev = clean(read('requirements.dev.txt').split())
package = json.loads(read('package.json'))


# Run
setup(
    name=package['name'],
    version=package['version'],
    description=package['description'],
    long_description=readme,
    author=package['author'],
    author_email=package['author_email'],
    url=package['repository'],
    license=package['license'],
    include_package_data=True,
    packages=find_packages(exclude=['examples', 'tests']),
    package_dir={package['slug']: package['slug']},
    install_requires=requirements,
    tests_require=requirements_dev,
    test_suite='nose.collector',
    zip_safe=False,
    keywords=package['keywords'],
    classifiers=package['classifiers'],
)
