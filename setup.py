from setuptools import setup, find_packages
import codecs
import os

here = os.path.abspath(os.path.dirname(__file__))

VERSION = '0.0.2'
DESCRIPTION = 'Base package to test custom rgsync'
LONG_DESCRIPTION = 'Base version'

# Setting up
setup(
    name="redis_write_behind",
    version=VERSION,
    author="Manideep",
    author_email="manideep.gujjari@gmail.com",
    description=DESCRIPTION,
    long_description_content_type="text/markdown",
    long_description="",
    packages=find_packages(),
    install_requires=[],
    classifiers=[
        "Development Status :: 1 - Planning",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)