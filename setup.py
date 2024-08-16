from glob import glob
from pathlib import Path
from setuptools import setup, find_packages

PACKAGE_NAME = 'ds-dag'
PACKAGE_VERSION = '1.0.0'

# Function to read the requirements from requirements.txt
def read_requirements(filename):
    with open(Path(__file__).resolve().parent / filename, 'r') as req_file:
        return req_file.read().splitlines()

install_requires = read_requirements('requirements.txt')
test_requires = read_requirements('requirements-test.txt')

setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    author='Gil Reich',
    author_email='gilr@wix.com',
    url='https://github.com/wix-private/ds-general/ds-dag',
    packages=find_packages("."),
    install_requires=install_requires,
    extras_require=dict(tests=test_requires),
    python_requires='>=3.10',
    data_files=[('docs', glob('docs/*.md'))],
    include_package_data=True,
)
