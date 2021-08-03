#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer

requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-orc',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Intake ORC plugin',
    maintainer='Mediahuis Data Team',
    maintainer_email='dnm-data@mediahuis.be',
    license='BSD',
    packages=find_packages(),
    entry_points={
        'intake.drivers': ['orc = intake_orc.source:ORCSource']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.md').read(),
    zip_safe=False,
)
