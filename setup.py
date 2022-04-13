from setuptools import setup

with open("README.md", encoding='utf-8') as fh:
    long_description = fh.read()


setup(
    name='pyTigerGraph',
    packages=['pyTigerGraph', 'pyTigerGraph.gds'],
    package_data={'pyTigerGraph.gds': ['gsql/dataloaders/*.gsql']},
    include_package_data=True,
    version='0.0.9.9.2',
    license='MIT',
    description='Library to connect to TigerGraph databases',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='TigerGraph Inc.',
    author_email='support@tigergraph.com',
    url='https://www.tigergraph.com/',
    download_url='',
    keywords=['TigerGraph', 'Graph Database'],
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
)
