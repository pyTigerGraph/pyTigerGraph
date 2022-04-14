import pathlib

from setuptools import find_packages, setup

# Get absolute path to the decription file to avoid reading in
# something unexpected.
here = pathlib.Path(__file__).parent.resolve()
long_description = (here / "README.md").read_text(encoding="utf-8")

# Get version number from a single source of truth
def get_version(version_path):
    with open(version_path) as infile:
        for line in infile:
            if line.startswith('__version__'):
                delim = '"' if '"' in line else "'"
                return line.split(delim)[1]
        else:
            raise RuntimeError("Unable to find version string.")

#TODO: Update url and email, project_urls

setup(
    name='pyTigerGraph',
    packages=find_packages(where="."),
    package_data={'': ['*']},
    version=get_version(here/"pyTigerGraph"/"__init__.py"),
    license='MIT',
    description='Library to connect to TigerGraph databases',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='TigerGraph Inc.',
    author_email='support@tigergraph.com', 
    url='https://www.tigergraph.com/',
    download_url='',
    keywords=['TigerGraph', 'Graph Database', 'Data Science', 'Machine Learning'],
    install_requires=[
        'pyTigerDriver',
        'validators',
        'requests',
        'pandas'],
    classifiers=[
        'Development Status :: 4 - Beta',  # 3 - Alpha, 4 - Beta or 5 - Production/Stable
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    extras_require={
        "gds-pyg": ["kafka-python", "numpy", "torch", "torch-sparse", "torch-scatter", "torch-geometric"],
        "gds-dgl": ["kafka-python", "numpy", "torch", "dgl"],
        "gds-lite": ["kafka-python", "numpy"],
        "gds": ["kafka-python", "numpy", "torch", "torch-sparse", "torch-scatter", "torch-geometric", "dgl"]
    },
    project_urls={  
        "Bug Reports": "https://github.com/pyTigerGraph/pyTigerGraph/issues",
        "Source": "https://github.com/pyTigerGraph/pyTigerGraph",
    },
)
