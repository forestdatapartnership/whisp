from setuptools import setup, find_packages

setup(
    name="whisp",
    version="0.0.1 Alpha",
    author="Andy Arnell",
    author_email="andrew.arnell@fao.org",
    description="Whisp (What is in that plot) is an open-source solution which helps to produce relevant forest monitoring information and support compliance with deforestation-related regulations.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/forestdatapartnership/whisp",
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
    install_requires=[
        "earthengine-api",
        "geemap",
        "numpy<2",
        "pandas",
        "pandera",
        "country_converter",
        "geojson",
        "python-dotenv",
        "setuptools",
    ],
    extras_require={
        "dev": ["pytest", "pre-commit", "ruff"],
    },
    classifiers=[
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
