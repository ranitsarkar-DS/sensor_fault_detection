from setuptools import find_packages,setup
from typing import List

NAME="sensor"
VERSION="0.0.2"
AUTHOR="Ranit"
AUTHOR_EMAIL_ID="example@gmail.com"

REQUIREMENTS_FILE_NAME="requirements.txt"
HYPEN_E_DOT="-e."

def get_requirements(file_path:str=REQUIREMENTS_FILE_NAME)->List[str]:
    requirements = []

    with open(file_path) as file_obj:
        requirements = file_obj.readlines()
        requirements = [req.replace("\n","") for req in requirements]

    if HYPEN_E_DOT in requirements:
        requirements.remove(HYPEN_E_DOT)

    return requirements


setup(
    name=NAME,
    version=VERSION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL_ID,
    packages=find_packages(),
    install_requires=get_requirements(),
)