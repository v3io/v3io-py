try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


def version():
    with open('v3io/__init__.py') as fp:
        for line in fp:
            if '__version__' in line:
                _, version = line.split('=')
                return version.replace("'", '').strip()


def load_deps(section):
    """Load dependencies from Pipfile, we can't assume toml is installed"""
    # [packages]
    header = '[{}]'.format(section)
    with open('Pipfile') as fp:
        in_section = False
        for line in fp:
            line = line.strip()
            if not line:
                continue

            if line == header:
                in_section = True
                continue

            if line.startswith('['):
                in_section = False
                continue

            if in_section:
                # ipython = ">=6.5"
                i = line.find('=')
                assert i != -1, 'bad dependency - {}'.format(line)
                pkg = line[:i].strip()
                version = line[i+1:].strip().replace('"', '')
                if version == '*':
                    yield pkg
                else:
                    yield '{}{}'.format(pkg, version.replace('"', ''))


install_requires = list(load_deps('packages'))
tests_require = list(load_deps('dev-packages'))

with open('README.md') as fp:
    long_desc = fp.read()


setup(
    name='v3io',
    version=version(),
    description='SDK for v3io',
    long_description=long_desc,
    long_description_content_type='text/markdown',
    author='Eran Duchan',
    author_email='erand@iguazio.com',
    license='MIT',
    url='https://github.com/v3io/v3io-py',
    packages=[
        'v3io',
        'v3io.common',
        'v3io.dataplane',
        'v3io.dataplane.transport',
        'v3io.logger'
    ],
    install_requires=install_requires,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Software Development :: Libraries',
    ],
    tests_require=tests_require,
)
