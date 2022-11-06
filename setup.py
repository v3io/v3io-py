# Copyright 2019 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def version():
    with open("v3io/__init__.py") as fp:
        for line in fp:
            if "__version__" in line:
                _, version = line.split("=")
                return version.replace("'", "").strip()


def load_deps(file_name):
    """Load dependencies from requirements file"""
    deps = []
    with open(file_name) as fp:
        for line in fp:
            line = line.strip()
            if not line or line[0] == "#":
                continue
            deps.append(line)
    return deps


install_requires = load_deps("requirements.txt")
tests_require = load_deps("dev-requirements.txt")

with open("README.md") as fp:
    long_desc = fp.read()

extras_require = {
    "aiohttp": ["aiohttp~=3.8"],
}

setup(
    name="v3io",
    version=version(),
    description="SDK for v3io",
    long_description=long_desc,
    long_description_content_type="text/markdown",
    author="Eran Duchan",
    author_email="erand@iguazio.com",
    license="MIT",
    url="https://github.com/v3io/v3io-py",
    packages=[
        "v3io",
        "v3io.common",
        "v3io.dataplane",
        "v3io.dataplane.transport",
        "v3io.aio.dataplane",
        "v3io.aio.dataplane.transport",
        "v3io.logger",
    ],
    install_requires=install_requires,
    extras_require=extras_require,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: POSIX :: Linux",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Software Development :: Libraries",
    ],
    tests_require=tests_require,
)
