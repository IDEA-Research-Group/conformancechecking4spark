import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="conformancechecking4spark",
    version="0.1.0",
    author="Alvaro Valencia-Parra",
    author_email="alvarovalenciavp@gmail.com",
    description="A library for performing conformance checking on large event logs and complex PNML models by using "
                "pm4py and Apache Spark.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://www.idea.us.es/confcheckingbigdata/",
    package_data={'': ['src/jobs/data']},
    include_package_data=True,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)