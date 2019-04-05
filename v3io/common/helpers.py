def url_join(base, *parts):
    result = base

    if result[0] != '/':
        result = '/' + base

    for part in parts:
        if part[0] != '/':
            result += '/' + part
        else:
            result += part

    return result
