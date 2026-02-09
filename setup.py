from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in tdgl_streaming/__init__.py
from tdgl_streaming import __version__ as version

setup(
	name="tdgl_streaming",
	version=version,
	description="Streaming service for TDGL instances",
	author="Percival Rapha",
	author_email="percival.rapha@gmail.com",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
