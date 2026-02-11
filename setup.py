import setuptools

setuptools.setup(
    name="dataflow-mongo",
    version="1.0",
    install_requires=[
        "apache-beam[gcp]",
        "pymongo",
        "dnspython"
    ],
    packages=setuptools.find_packages(),
)