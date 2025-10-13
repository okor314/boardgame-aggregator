def errorCatcher(func, heandler, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except Exception as e:
        return heandler(e)
    
def saveTo(path, data: list, mode = None):
    if mode == 'newfile':
        # Clear file and write column names
        with open(path, 'w', encoding='utf-8') as f:
            col_names = data[0].keys()
            f.write(','.join(col_names))
        # Regular saving data
        saveTo(path, data)
    else:
        with open(path, 'a', encoding='utf-8') as f:
            for item in data:
                values = [str(value) for value in item.values()]
                f.write('\n'+','.join(values))