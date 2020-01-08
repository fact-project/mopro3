from setuptools import setup, find_packages
import os


setup_dir = os.path.dirname(__file__)
with open(os.path.join(setup_dir, 'README.md')) as f:
    long_description = f.read()


setup(
    name='mopro',
    description='FACT Monte Carlo Production',
    long_description=long_description,
    version='3.0.2',
    author='Maximilian Nöthe',
    author_email='maximilian.noethe@tu-dortmund.de',
    packages=find_packages(),
    install_requires=[
        'ruamel.yaml',
        'click',
        'peewee~=3.8',
        'pandas',
        'retrying',
        'jinja2',
        'pyzmq',
        'pymysql',
    ],
    setup_requires=['pytest_runner'],
    entry_points={
        'console_scripts': [
            'mopro_install_root = mopro.installation.root:main',
            'mopro_install_mars = mopro.installation.mars:main',
            'mopro_install_corsika = mopro.installation.corsika:main',
        ],
    },
    package_data={'mopro': ['resources/*']},
)
