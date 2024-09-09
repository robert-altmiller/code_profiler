import subprocess
import sys

def install_requirements(requirements_file='./requirements.txt'):
    """install the requirements text file"""
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_file])
        print(f"Successfully installed packages from {requirements_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while installing packages: {e}")

install_requirements()