# coding: utf-8

from setuptools import setup

version = '0.1'

setup(name='MDark',
      version=version,
      description="Mini DPark for learning.",
      long_description=open("README.md").read(),
      classifiers=[
        "Programming Language :: Python",
      ],
      keywords='dpark python mapreduce spark',
      author='zzl',
      author_email='zhuzhaolong0@gmail.com',
      license= 'MIT License',
      packages=['mdpark'],
      include_package_data=True,
      zip_safe=False,
      install_requires=[
          'setuptools',
      ],
      tests_require=[
          'nose',
      ],
      test_suite='nose.collector',
)