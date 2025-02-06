from setuptools import setup, find_packages

setup(
    name="whisp",
    version="0.0.1 Alpha",
    description="A description of the Whisp package",
    author="Andy Arnell",
    author_email="andrew.arnell@fao.org",
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
