from setuptools import setup, find_packages
import os

here = os.path.abspath(os.path.dirname(__file__))

VERSION = '0.6.0'
DESCRIPTION = 'Base package to test custom redis write behind gears pipelines'
LONG_DESCRIPTION = 'Version 0.6'

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
        "Development Status :: Version 1.1",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Operating System :: Unix",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: Microsoft :: Windows",
    ]
)