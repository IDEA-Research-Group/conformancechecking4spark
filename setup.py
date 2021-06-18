import setuptools

#with open("README.md", "r") as fh:
#    long_description = fh.read()

setuptools.setup(
    name="conformancechecking4spark",
    version="0.3.0",
    author="Alvaro Valencia-Parra",
    author_email="alvarovalenciavp@gmail.com",
    description="A library for performing conformance checking on large event logs and complex PNML models in a "
                "distributed environment by using pm4py and Apache Spark.",
    long_description_content_type="text/markdown",
    url="https://github.com/IDEA-Research-Group/conformancechecking4spark",
    download_url="https://github.com/IDEA-Research-Group/conformancechecking4spark/tarball/0.1",
    classifiers=[],
    keywords=["conformancecheking", "pm4py", "conformance checking", "process mining", "apache spark", "spark",
              "distributed process mining"],
    python_requires='>=3.7',
    packages=setuptools.find_packages()
)
