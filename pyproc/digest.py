import pyhash

# Read up to 1MB at a time
BLOCK_SIZE = 2**20
# Use a 128-bit CityHash
hasher = pyhash.city_128()

def get_hash(filename):
    """Get a hash of this file (cryptographic hash not necessary here)"""
    # Set up the hash function
    value = 0L
    with open(filename, 'rb') as handle:
        # Read the data one block at a time
        buf = handle.read(BLOCK_SIZE)
        while len(buf) > 0:
            # Hash each block, using the previous output as the seed
            value = hasher(buf, seed=value)
            buf = handle.read(BLOCK_SIZE)
    # Return the final output
    return value
