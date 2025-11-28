def errorCatcher(func, heandler, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        return heandler(e)
    
def formatValues(values: list):
    """Checks if values acceptable for CSV file
    and corrects if not"""
    # Replacing double-quote with two double-quote characters
    values = [val.replace('"', '""') for val in values]
    # Quoting fields containing special characters
    values = ['"'+val+'"' if ',' in val or '"' in val else val for val in values]
    return values

def saveTo(path, data: list, mode = None, columns: list = []):
    if data == [] and columns == []: return
    if mode == 'newfile':
        filemode = 'w'
        if columns:
            # Clear file and write column names
            with open(path, filemode, encoding='utf-8') as f:
                f.write(','.join(columns))
            filemode = 'a'
        if data:
            with open(path, filemode, encoding='utf-8') as f:
                pass
            saveTo(path, data)

    else:
        with open(path, 'a', encoding='utf-8') as f:
            for item in data:
                values = [str(value) if value is not None else '' for value in item.values()]
                values = formatValues(values)
                f.write('\n'+','.join(values))

if __name__ == '__main__':
    saveTo('./data/test.csv', [{'a':1, 'b':2}, {'a':3, 'b':4}], 'newfile', ['a', 'b'])