import pickle
from dbdb.logical import LogicalBase, ValueRef

class BinaryNodeRef(ValueRef):
    def prepare_to_store(self, storage):
        """
        Подготавливает BinaryNode к сохранению.
        """
        if self._referent:
            print(f"Preparing to store node with key: {self._referent.key}")  # Отладочное сообщение
            self._referent.store_refs(storage)

    def referent_to_string(self, referent):
        """
        Сериализует BinaryNode в строку для сохранения в хранилище.
        """
        try:
            return pickle.dumps({
                'left': referent.left_ref.address,
                'key': referent.key,
                'value': referent.value_ref.address,
                'right': referent.right_ref.address,
                'length': referent.length,
            })
        except pickle.PicklingError as e:
            raise ValueError(f"Failed to serialize node: {e}")

    def string_to_referent(self, string):
        """
        Десериализует строку в BinaryNode.
        """
        try:
            if not string:
                raise ValueError("Empty data cannot be deserialized")
            data = pickle.loads(string)
            return BinaryNode(
                self.__class__(address=data['left']),
                data['key'],
                ValueRef(address=data['value']),
                self.__class__(address=data['right']),
                data['length']
            )
        except pickle.UnpicklingError as e:
            raise ValueError(f"Failed to deserialize node: {e}")
        except Exception as e:
            raise ValueError(f"Unexpected error during deserialization: {e}")

class BinaryNode(object):
    def __init__(self, left_ref, key, value_ref, right_ref, length):
        self.left_ref = left_ref
        self.key = key
        self.value_ref = value_ref
        self.right_ref = right_ref
        self.length = length

    def store_refs(self, storage):
        """
        Сохраняет ссылки на дочерние узлы в хранилище.
        """
        print(f"Storing refs for node with key: {self.key}")  # Отладочное сообщение
        self.value_ref.store(storage)
        self.left_ref.store(storage)
        self.right_ref.store(storage)

    @classmethod
    def from_node(cls, node, left_ref=None, right_ref=None, value_ref=None):
        """
        Создаёт новый узел на основе существующего, с возможностью замены ссылок.
        """
        if left_ref is None:
            left_ref = node.left_ref
        if right_ref is None:
            right_ref = node.right_ref
        if value_ref is None:
            value_ref = node.value_ref
        return cls(left_ref, node.key, value_ref, right_ref, node.length)

class BinaryTree(LogicalBase):
    node_ref_class = BinaryNodeRef
    value_ref_class = ValueRef

    def _follow(self, ref):
        return ref.get(self._storage)

    def _get(self, node, key):
        while node is not None:
            if key < node.key:
                node = self._follow(node.left_ref)
            elif node.key < key:
                node = self._follow(node.right_ref)
            else:
                return self._follow(node.value_ref)
        raise KeyError(f"Key not found: {key}")

    def _insert(self, node, key, value_ref):
        if node is None:
            new_node = BinaryNode(
                self.node_ref_class(), key, value_ref, self.node_ref_class(), 1)
        elif key < node.key:
            new_node = BinaryNode.from_node(
                node,
                left_ref=self._insert(
                    self._follow(node.left_ref), key, value_ref))
        elif node.key < key:
            new_node = BinaryNode.from_node(
                node,
                right_ref=self._insert(
                    self._follow(node.right_ref), key, value_ref))
        else:
            new_node = BinaryNode.from_node(node, value_ref=value_ref)
        return self.node_ref_class(referent=new_node)

    def _delete(self, node, key):
        if node is None:
            raise KeyError(f"Key not found: {key}")
        elif key < node.key:
            new_node = BinaryNode.from_node(
                node,
                left_ref=self._delete(
                    self._follow(node.left_ref), key))
        elif node.key < key:
            new_node = BinaryNode.from_node(
                node,
                right_ref=self._delete(
                    self._follow(node.right_ref), key))
        else:
            if node.left_ref is None:
                return node.right_ref
            elif node.right_ref is None:
                return node.left_ref
            else:
                min_node = self._find_min(self._follow(node.right_ref))
                new_node = BinaryNode.from_node(
                    node,
                    right_ref=self._delete(
                        self._follow(node.right_ref), min_node.key),
                    value_ref=min_node.value_ref)
        return self.node_ref_class(referent=new_node)

    def _find_min(self, node):
        while node.left_ref is not None:
            node = self._follow(node.left_ref)
        return node

    def get_all(self):
        """
        Возвращает все ключи и значения в дереве.
        """
        result = []
        if self._tree_ref is not None:  # Проверяем, что дерево не пустое
            print(f"Tree ref address: {self._tree_ref.address}")  # Отладочное сообщение
            root_node = self._follow(self._tree_ref)
            if root_node is not None:
                print(f"Root node key: {root_node.key}")  # Отладочное сообщение
                self._get_all(root_node, result)
            else:
                print("Root node is None.")  # Отладочное сообщение
        else:
            print("Tree ref is None.")  # Отладочное сообщение
        return result

    def _get_all(self, node, result):
        """
        Рекурсивно обходит дерево и добавляет ключи и значения в result.
        """
        if node is not None:
            print(f"Processing node with key: {node.key}")  # Отладочное сообщение
            # Обходим левое поддерево
            self._get_all(self._follow(node.left_ref), result)
            # Добавляем текущий ключ и значение
            value = self._follow(node.value_ref)
            print(f"Key: {node.key}, Value: {value}")  # Отладочное сообщение
            result.append((node.key, value))
            # Обходим правое поддерево
            self._get_all(self._follow(node.right_ref), result)