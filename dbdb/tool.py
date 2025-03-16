import sys
import dbdb

def main(argv):
    if len(argv) < 3:
        return print('BAD_ARGS: Usage: python -m dbdb.tool <dbname> <verb> [key] [value]')
    
    dbname = argv[1]
    verb = argv[2]
    
    # Для команд 'get', 'set', 'delete' требуется ключ
    if verb in {'get', 'set', 'delete'}:
        if len(argv) < 4:
            return print('BAD_ARGS: Key is required for get, set, delete')
        key = argv[3]
        value = argv[4] if verb == 'set' and len(argv) > 4 else None
    elif verb == 'list':
        # Для команды 'list' ключ и значение не требуются
        key = None
        value = None
    else:
        return print('BAD_VERB: Verb must be "get", "set", "delete", or "list"')
    
    db = dbdb.connect(dbname)  # CONNECT
    try:
        if verb == 'get':
            result = db[key]  # GET VALUE
            print(result)  # Вывод результата
        elif verb == 'set':
            db[key] = value
            db.commit()
            print(f"Value '{value}' set for key '{key}'")
        elif verb == 'delete':
            del db[key]
            db.commit()
            print(f"Key '{key}' deleted")
        elif verb == 'list':  # Обработка команды 'list'
            all_data = db.list_all()  # Получаем все ключи и значения
            if all_data:
                for k, v in all_data:
                    print(f"Key: {k}, Value: {v}")
            else:
                print("Database is empty.")
    except KeyError:
        print(f"Key '{key}' not found", file=sys.stderr)
        return print('BAD_KEY')
    return print('OK')

if __name__ == "__main__":
    main(sys.argv)