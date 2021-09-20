import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Novigi_Operators",                       # This is the name of the package
    version="1.0",                                 # The initial release version
    author="Novigi",                               # Full name of the author
    author_email = 'prasadi.jayakodi@novigi.com.au', # Type in your E-Mail
    url = 'https://Prasadi1994@bitbucket.org/novigi/novigi_operators.git',   #Bitbucket repo link
    keywords = ['Novigi', 'Custom' ,'Operators' ], # Keywords that define our package best
    description="Novigi Custom Airflow Operators",
    long_description=long_description,             # Long description read from the the readme file
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),           # List of all python modules to be installed
    classifiers=[                                  # Information to filter the project on PyPi website
    'License :: OSI Approved :: MIT License',      # Pick a license
    'Programming Language :: Python :: 2.7',       # Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3',       
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Operating System :: OS Independent',
    ],                                   
    py_modules=["Novigi_Operators"],             # Name of the python package
    install_requires=[                           # Install other dependencies if any
                'requests',
                'jsonpath_ng',
                'pandas'
    ]                 
)