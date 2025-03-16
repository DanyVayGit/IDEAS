import portalocker
import struct

class Storage(object):
    SUPERBLOCK_SIZE = 4096
    INTEGER_FORMAT = "!Q"  # Формат для записи длины данных (8 байт, беззнаковое целое)
    INTEGER_LENGTH = 8

    def __init__(self, f):
        self._f = f
        self.locked = False
        self._ensure_superblock()

    def _ensure_superblock(self):
        self.lock()
        self._seek_end()
        end_address = self._f.tell()
        if end_address < self.SUPERBLOCK_SIZE:
            self._f.write(b'\x00' * (self.SUPERBLOCK_SIZE - end_address))
        self.unlock()

    def lock(self):
        if not self.locked:
            portalocker.lock(self._f, portalocker.LOCK_EX)
            self.locked = True
            return True
        return False

    def unlock(self):
        if self.locked:
            portalocker.unlock(self._f)
            self.locked = False

    def _seek_end(self):
        self._f.seek(0, 2)

    def _seek_superblock(self):
        self._f.seek(0)

    def _write_integer(self, integer):
        self._f.write(struct.pack(self.INTEGER_FORMAT, integer))

    def _read_integer(self):
        return struct.unpack(self.INTEGER_FORMAT, self._f.read(self.INTEGER_LENGTH))[0]

    def get_root_address(self):
        self._seek_superblock()
        return self._read_integer()

    def commit_root_address(self, root_address):
        self.lock()
        self._f.flush()
        self._seek_superblock()
        print(f"Writing root address: {root_address}")  # Отладочное сообщение
        self._write_integer(root_address)
        self._f.flush()
        self.unlock()

    def write(self, data):
        self.lock()
        self._seek_end()
        # Записываем длину данных перед самими данными
        length = len(data)
        if length < 0:
            raise ValueError("Data length cannot be negative")
        self._f.write(struct.pack(self.INTEGER_FORMAT, length))
        self._f.write(data)
        self.unlock()
        return self._f.tell() - length - self.INTEGER_LENGTH  # Возвращаем адрес начала данных

    def read(self, address):
        self._f.seek(address)
        # Читаем длину данных
        length_data = self._f.read(self.INTEGER_LENGTH)
        if not length_data:
            raise ValueError("No data found at the specified address")
        try:
            length = struct.unpack(self.INTEGER_FORMAT, length_data)[0]
        except struct.error as e:
            raise ValueError(f"Failed to unpack data length: {e}")
        # Проверяем, что длина данных корректна
        if length < 0 or length > 10**9:  # Ограничиваем максимальную длину данных
            raise ValueError(f"Invalid data length: {length}")
        # Читаем сами данные
        data = self._f.read(length)
        if len(data) != length:
            raise ValueError(f"Incomplete data read from storage: expected {length}, got {len(data)}")
        return data

    @property
    def closed(self):
        return self._f.closed