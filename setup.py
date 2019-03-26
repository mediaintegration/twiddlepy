from setuptools import setup, find_packages
import os

thelibFolder = os.path.dirname(os.path.realpath(__file__))
requirementPath = thelibFolder + '/requirements.txt'
install_requires = [] # Examples: ["gunicorn", "docutils>=0.3", "lxml==0.5a7"]
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        install_requires = f.read().splitlines()


print(find_packages())
setup(
    name='twiddlepy',
    version='0.0.1',
    description='Extract, Transform and Load pipeline application',
    author='MIT',
    author_email='awburt@btopenworld.com',
    packages=find_packages(),
    install_requires=install_requires,
    include_package_data=True,
    entry_points = {
        'console_scripts': ['twiddle=twiddle.command_line:main'],
    }
)