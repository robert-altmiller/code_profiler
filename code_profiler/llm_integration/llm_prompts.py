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
You are submitting the following Python code for an automated code review system that strictly accepts only the final optimized code version and is formatted according to PEP-8 and Pythonic best practices. 

Please reformat and optimize the code below to ensure it meets the system's strict criteria:
- Focus on performance.
- Enhance readability and maintainability.
- Adhere strictly to Python idioms and conventions.

Your submission should mimic a direct code commit in a professional software development environment, including appropriate docstrings.

The returned answer should ONLY be the optimized function definition.  Do NOT include any explanations for the optimized function definition.

Here is the code below to optimize, and remember, the returned answer is ONLY the optimized function definition:\n\n
'''