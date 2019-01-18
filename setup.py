from setuptools import setup, find_packages
import os


setup_dir = os.path.dirname(__file__)
with open(os.path.join(setup_dir, 'README.md')) as f:
    long_description = f.read()


setup(
    name='mopro',
    description='FACT Monte Carlo Production',
    long_description=long_description,
    version='3.0.0a0',
    author='Maximilian Nöthe',
    author_email='maximilian.noethe@tu-dortmund.de',
    packages=find_packages(),
    install_requires=[
        'ruamel.yaml',
        'click',
    ]
)
