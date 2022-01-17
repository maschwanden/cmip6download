import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cmip6download",
    version="0.1.0",
    author="Mathias Aschwanden",
    author_email="mathias.aschwanden@gmail.com",
    description="Download tool for CMIP6 data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    install_requires=[
        'requests',
        'PyYAML',
        'beautifulsoup4',
        'lxml',
    ],
)
