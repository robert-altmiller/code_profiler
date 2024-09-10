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
code_opt_prompt = '''
Please optimize the following Python code with a focus on performance, readability, and 
adherence to Pythonic best practices (e.g., PEP-8 standards, minimizing redundancy, and using built-in functions where appropriate).  
The optimized code should be returned as a clean, uninterrupted Python function without additional comments, explanations, or any formatting other than the code itself.

- Aim to improve runtime efficiency and reduce total cost of ownership (TCO) where possible.
- Enhance code clarity and maintainability.
- Adhere to Python idioms and conventions (e.g., list comprehensions, proper variable naming, etc.)

Return the optimized function as a complete block of code, formatted according to Pythonic standards without any accompanying text or explanations.

Here is the code below to optimize:\n\n
'''
