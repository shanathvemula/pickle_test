from setuptools import setup, find_packages

setup(
    name='pickle_test',
    version='0.0.15',
    packages=find_packages(),
    install_requires=['PySide6>=6.5.0', 'pandas>=2.1.4', 'psycopg2>=2.9.9', 'setuptools>=60.2.0', 'pymongo>=4.6.1'],
    download_url='https://github.com/UN-GCPDS/qt-material',
)
