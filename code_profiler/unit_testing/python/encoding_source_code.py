import zlib, base64

source_code ="ABCDEFGHIJK"
source_code_compressed = zlib.compress(source_code.encode('utf-8'))
source_code_compressed = base64.b64encode(source_code_compressed).decode('utf-8')

source_code_decompressed = base64.b64decode(source_code_compressed)
source_code_decompressed = zlib.decompress(source_code_decompressed).decode('utf-8')

print(source_code_compressed)
print(source_code_decompressed)