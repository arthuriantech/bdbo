import os
import sys
from setuptools import setup, find_packages

scriptdir = os.path.dirname(os.path.abspath(__file__))

with open(os.path.join(scriptdir, 'README.md')) as f:
    long_description = f.read()

setup(
    name='bdbo',
    version='0.0.1',
    author='Arthur Goncharuk',
    author_email='af3.inet@gmail.com',
    description='Experimental wrapper for the bsddb3',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/arthuriantech/bdbo',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3 :: Only',
        'Operating System :: OS Independent',
    ],
    install_requires=['bsddb3'],
    python_requires='>=3.5',
    zip_safe=False
)
