import zlib, base64

source_code = '''
    def is_prime(n):
        """Check if a number is prime. """
        if n <= 1:
            return False
        for i in range(2, int(n**0.5) + 1):
            if n % i == 0:
                return False
        return True
'''

source_code_compressed = zlib.compress(source_code.encode('utf-8'))
source_code_compressed = base64.b64encode(source_code_compressed).decode('utf-8')
source_code_decompressed = base64.b64decode(source_code_compressed)
source_code_decompressed = zlib.decompress(source_code_decompressed).decode('utf-8')

print(source_code_compressed)
print(source_code_decompressed)