import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="cmip6download",
    version="0.0.1",
    author="Mathias Aschwanden",
    author_email="mathias.aschwanden@gmail.com",
    description="Tools to simplify data download of CMIP6 data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    #url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    install_requires=[
        'requests',
    ],
)
