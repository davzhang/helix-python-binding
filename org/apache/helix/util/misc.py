''' misc utilities
'''

def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums['reverse_mapping'] = reverse
    def reverse_m(value):
        return reverse[value]
    enums['toString'] = staticmethod(reverse_m)
    return type('Enum', (), enums)


def explicit_enum(**enums):
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums['reverse_mapping'] = reverse
    def reverse_m(value):
        return reverse[value]
    enums['toString'] = staticmethod(reverse_m)
    return type('Enum', (), enums)

def ternary(cond1, result1, result2):
    if cond1:
        return result1
    else:
        return result2
