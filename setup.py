# filepath: /c:/Users/Arnell/Documents/GitHub/testpypi/whisp/setup.py
from setuptools import setup, find_packages

# Function to read the requirements.txt file
def read_requirements(filename):
    with open(filename, encoding="utf-8") as req_file:
        return req_file.read().splitlines()

setup(
    name="whisp",
    version="0.0.1",
    author="Andy Arnell",
    author_email="andrew.arnell@fao.org",
    description="Whisp (What is in that plot) is an open-source solution which helps to produce relevant forest monitoring information and support compliance with deforestation-related regulations.",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/andyarnell/whisp",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.10",
    include_package_data=True,
    package_data={
        "whisp.parameters": [
            "lookup_gee_datasets.csv",
            "lookup_context_and_metadata.csv",
        ]
    },
    install_requires=read_requirements("requirements.txt"),
    extras_require={
        "dev": read_requirements("requirements-dev.txt"),
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
    ],
)