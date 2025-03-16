

import subprocess


command_get = 'python -m dbdb.tool base.db get test'

command_set = 'python -m dbdb.tool base.db set test test'

# Выполнить команду bash
result = subprocess.run(['bash', '-c', command_get], capture_output=True, text=True)

# Вывод результата
print(result.stdout)
