from setuptools import setup

setup(
    name='panacea-crawl',
    version='1.0.0',
    description='A distributed crawling library with easy crawling features',
    url='https://github.com/Rishi16/python_crawling_library',
    author='Rishikesh Shendkar',
    author_email='rishikesh.shendkar@gmail.com',
    license='',
    # packages=['python_crawling_library'],
    install_requires=['fake_headers',
                      'selenium',
                      'amazoncaptcha',
                      'selenium-wire',
                      'selenium',
                      'lxml',
                      'pandas',
                      'stem',
                      'requests',
                      'psycopg2',
                      'psutil'
                      ],

    classifiers=[
        'Intended Audience :: Science/Research',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.9',
    ],
)
