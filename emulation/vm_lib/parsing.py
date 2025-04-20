import re


units = {
    "B": 1,
    "KiB": 2**10,
    "MiB": 2**20,
    "GiB": 2**30,
    "TiB": 2**40
}

def parse_size(size):
    if not re.match(r' ', size):
        size = re.sub(r'([KMGT]?iB)', r' \1', size)
    number, unit = [string.strip() for string in size.split()]
    return int(float(number)*units[unit])
