from setuptools import setup

setup(
    name='quazydb',
    version='1.0',
    packages=['quazy'],
    url='https://github.com/zergos/quazydb',
    license='Apache 2.0',
    author='Andrey Aseev',
    author_email='invent@zergos.ru',
    description='Powerful yet simple Python ORM',
    install_requires=[
        'psycopg>=3.0.1',
        'psycopg-pool>=3.0',
        'PyYAML>=6.0.1',
        'jsonpickle>=2.0.0',
    ],
    python_requires=">=3.10",
)
