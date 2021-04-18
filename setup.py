from setuptools import setup, Extension

with open("README.md", encoding='utf-8') as fh:
    long_description = fh.read()

setup(
  name = 'pyTigerGraph',         # How you named your package folder (MyLib)
  packages = ['pyTigerGraph'],   # Chose the same as "name"
  version = '0.0.9.6.4',      # Start with a small number and increase it with every change you make
  license='MIT',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'Library to connect to TigerGraph databases',   # Give a short description about your library
  long_description=long_description,
  long_description_content_type='text/markdown',
  author = 'Zrouga Mohamed / Jonathan Herke / Parker Erickson / TigerGraph',                   # Type in your name
  author_email = 'medzrouga@gmail.com',      # Type in your E-Mail
  url = 'https://github.com/pyTigerGraph/pyTigerGraph',   # Provide either the link to your github or to your website
  download_url = '',    # I explain this later on
  keywords = ['TigerGraph', 'Graph Database'],   # Keywords that define your package best
  install_requires=[            # I get to this in a second
          'pyTigerDriver==1.0.6',
          'validators',
          'requests',
          'pandas'],
  classifiers=[
    'Development Status :: 4 - Beta',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',   # Again, pick a license
    'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)
