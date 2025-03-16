import pickle

class LogicalBase(object):
    def __init__(self, storage):
        self._storage = storage
        self._tree_ref = None

    def get(self, key):
        if not self._storage.locked:
            self._refresh_tree_ref()
        return self._get(self._follow(self._tree_ref), key)

    def _refresh_tree_ref(self):
        root_address = self._storage.get_root_address()
        print(f"Refreshing tree ref. Root address: {root_address}")  # Отладочное сообщение
        if root_address is None:
            print("Root address is None. Tree is empty.")  # Отладочное сообщение
        self._tree_ref = self.node_ref_class(address=root_address)

    def set(self, key, value):
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._insert(
            self._follow(self._tree_ref), key, self.value_ref_class(value))

    def delete(self, key):
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._delete(
            self._follow(self._tree_ref), key)

    def commit(self):
        if self._tree_ref is not None:
            print(f"Storing tree ref. Address: {self._tree_ref.address}")  # Отладочное сообщение
            self._tree_ref.store(self._storage)
            print(f"Committing root address: {self._tree_ref.address}")  # Отладочное сообщение
            self._storage.commit_root_address(self._tree_ref.address)
        else:
            print("Tree ref is None. Nothing to commit.")  # Отладочное сообщение

    def _follow(self, ref):
        if ref is None:
            print("Ref is None.")  # Отладочное сообщение
            return None
        return ref.get(self._storage)

class ValueRef(object):
    def __init__(self, referent=None, address=None):
        self._referent = referent
        self._address = address

    @property
    def address(self):
        return self._address

    def prepare_to_store(self, storage):
        """
        Подготавливает данные к сохранению.
        Если _referent требует дополнительной обработки перед сохранением,
        этот метод должен быть переопределён в подклассах.
        """
        pass

    def store(self, storage):
        if self._referent is not None and not self._address:
            self.prepare_to_store(storage)
            serialized_data = self.referent_to_string(self._referent)
            self._address = storage.write(serialized_data)

    def referent_to_string(self, referent):
        """
        Сериализует referent в строку для сохранения в хранилище.
        Этот метод должен быть переопределён в подклассах.
        """
        # Используем pickle для сериализации данных
        return pickle.dumps(referent)

    def get(self, storage):
        """
        Возвращает referent, загружая его из хранилища, если необходимо.
        """
        if self._referent is None and self._address:
            data = storage.read(self._address)
            if not data:
                raise ValueError("No data found at the specified address")
            self._referent = self.string_to_referent(data)
        return self._referent

    def string_to_referent(self, string):
        """
        Десериализует строку в referent.
        Этот метод должен быть переопределён в подклассах.
        """
        # Используем pickle для десериализации данных
        try:
            if not string:
                raise ValueError("Empty data cannot be deserialized")
            return pickle.loads(string)
        except pickle.UnpicklingError as e:
            raise ValueError(f"Failed to deserialize data: {e}")
        except Exception as e:
            raise ValueError(f"Unexpected error during deserialization: {e}")