from setuptools import setup, find_packages
import os

thelibFolder = os.path.dirname(os.path.realpath(__file__))
requirementPath = thelibFolder + '/requirements.txt'
install_requires = [] # Examples: ["gunicorn", "docutils>=0.3", "lxml==0.5a7"]
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        install_requires = f.read().splitlines()

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name='twiddlepy',
    version='0.1.2',
    description='Extract, Transform and Load pipeline application',
    long_description=long_description,
    author='Media Integration Technologies Ltd.',
    author_email='info@mediaintegration.co.uk',
    url="https://github.com/mediaintegration/twiddlepy",
    packages=find_packages(exclude=["tests", "scripts"]),
    install_requires=install_requires,
    include_package_data=True,
    entry_points = {
        'console_scripts': ['twiddle=twiddle.command_line:main'],
    }
)
