# code recommendations prompt
code_recs_prompt = '''
Please provide suggestions in the form of a Python list on how to optimize the code below.
Each suggestion should be a string in a list format, without any numbering or additional text outside the list items.
The suggestions should focus on, but are not limited to, performance improvements, code readability, 
and adherence to Pythonic best practices. Do not include any optimized code or additional text in the return results.

For clarity, please ensure each suggestion is formatted as a direct list item. Example of expected format:
["suggestion one", "suggestion two", "suggestion three"]

Here is the code below:\n\n
'''

# optimized code prompt
# code_opt_prompt = '''
# Please optimize the following Python code with a focus solely on performance, readability, and adherence to 
# Pythonic best practices (e.g., PEP-8 standards, minimizing redundancy, using built-in functions where appropriate).
# The response should strictly be the optimized code itself without any explanations, narrative explanations, bullet points, or comments outside of the code.

# The optimized code should demonstrate:
# - Improved runtime efficiency and reduced total cost of ownership (TCO) where possible.
# - Enhanced code clarity and maintainability.
# - Strict adherence to Python idioms and conventions.

# Please guarantee the response is a single, uninterrupted block of Python code. DO NOT provide explanations, narrative explanations, bullet points, or comments outside of the Python code.

# Here is the code below to optimize:\n\n
# '''

# optimized code prompt

code_opt_prompt = '''
You are preparing a submission for an automated Python code evaluation system that rigorously enforces PEP-8 formatting standards, 
including correct indentation, proper use of whitespace, and Pythonic best practices. This system automatically rejects any code that fails to meet these strict formatting requirements.

Please ensure the code below is optimized for performance, readability, and maintainability, and that it strictly adheres to the required formatting standards:
- Optimize the code for better runtime efficiency.
- Enhance clarity and maintainability through clear and proper formatting.
- Follow all Python idioms and PEP-8 conventions rigorously, especially correct indentation.

Imagine you are committing this code directly into a highly strict codebase where formatting errors result in build failures. Your task is to rewrite and optimize the code below, including appropriate docstrings, but without any external commentary or explanations.

Here is the code below to optimize:\n\n
'''
