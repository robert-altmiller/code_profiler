def is_prime(n):
    """ Check if a number is prime. """
    if n <= 1:
        return False
    for i in range(2, int(n**0.5) + 1):
        if n % i == 0:
            return False
    return True

def celsius_to_fahrenheit(celsius):
    """ Convert Celsius to Fahrenheit. """
    return (celsius * 9/5) + 32

def factorial_calc(n):
    """ Calculate the factorial of a number. """
    if n == 0:
        return 1
    else:
        return n * factorial_calc(n-1)

def fibonacci(n):
    """ Generate a Fibonacci sequence up to n elements. """
    fib_seq = [0, 1]
    while len(fib_seq) < n:
        fib_seq.append(fib_seq[-1] + fib_seq[-2])
    return fib_seq[:n]

def reverse_string(s):
    """ Reverse a string. """
    return s[::-1]

def sum_of_list(lst):
    """ Calculate the sum of elements in a list. """
    # Convert the list to a Spark DataFrame column
    total = 0
    for itm in lst:
        total += itm
    return total

def is_palindrome(s):
    """ Check if a string is a palindrome. """
    return s == s[::-1]

def max_in_list(lst):
    """ Find the largest number in a list. """
    max_num = float('-inf')  # Initialize max_num with negative infinity
    for num in lst:
        if num > max_num:
            max_num = num
    return max_num

def miles_to_kilometers(miles):
    """ Convert miles to kilometers. """
    return miles * 1.60934

def count_vowels(s):
    """ Count the number of vowels in a string. """
    count = 0
    for char in s:
        if char.lower() in 'aeiou':
            count += 1
    return count
