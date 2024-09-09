import subprocess
import sys

def install_requirements(requirements_file='./requirements.txt'):
    """Install the requirements from the requirements.txt file using subprocess."""
    pip_command = [sys.executable, '-m', 'pip', 'install', '-r', requirements_file]
    try:
        subprocess.check_call(pip_command)
        print(f"Successfully installed packages from {requirements_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred while installing packages: {e}")

# Example: Run the function
install_requirements()
