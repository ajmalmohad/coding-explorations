import bisect
import hashlib

from consistent_hashing.manager.data_store import DataStore
from consistent_hashing.manager.visualization import Visualization


class ShardManager:
    def __init__(self, max_limit=2**32, virtual_nodes=150):
        self.shards_to_idx: dict[str, int] = {}
        self.virtual_to_physical: dict[str, str] = {}
        self.max_limit = max_limit
        self.virtual_nodes = virtual_nodes
        self.data_store = DataStore()
        self.viz = Visualization()

    def _hash(self, data: str) -> int:
        h = hashlib.sha256(data.encode("utf-8")).hexdigest()
        return int(h, 16) % self.max_limit

    def _find_node_for_hash(self, hash_val: int) -> str:
        if not self.shards_to_idx:
            raise ValueError("No nodes available in cluster")

        sorted_items = sorted(self.shards_to_idx.items(), key=lambda x: x[1])
        hashes = [h for _, h in sorted_items]
        idx = bisect.bisect_right(hashes, hash_val)
        virtual_node = sorted_items[idx % len(sorted_items)][0]
        return self.virtual_to_physical.get(virtual_node, virtual_node) 

    def get_data(self, id: str):
        node_name = self._find_node_for_hash(self._hash(id))
        return self.data_store.get_by_id(id, node_name)

    def insert_data(self, data: list[str]):
        id = data[0]
        node_name = self._find_node_for_hash(self._hash(id))
        self.data_store.insert(data, node_name)

    def delete_data(self, id: str):
        node_name = self._find_node_for_hash(self._hash(id))
        self.data_store.delete_by_id(id, node_name)

    def _rebalance_data_on_node_addition(self, node_name):
        existing_nodes = [p for p in set(self.virtual_to_physical.values()) if p != node_name]        
        for existing_node in existing_nodes:
            all_data = self.data_store.get_all(existing_node)
            for item in all_data.values.tolist():
                item_id = item[0]
                correct_node = self._find_node_for_hash(self._hash(item_id))
                if correct_node == node_name:
                    self.data_store.insert(item, node_name)
                    self.data_store.delete_by_id(item_id, existing_node)

    def _rebalance_data_on_node_removal(self, node_name):
        all_data = self.data_store.get_all(node_name)
        next_node = self._find_node_for_hash((self._hash(node_name) + 1) % self.max_limit)
        for item in all_data.values.tolist():
            self.data_store.insert(item, next_node)
            self.data_store.delete_by_id(item[0], node_name)

    def add_node(self, node_name: str):
        if node_name in self.virtual_to_physical.values():
            raise ValueError(f"Node {node_name} already exists.")
        
        for i in range(self.virtual_nodes):
            virtual_name = f"{node_name}#v{i}"
            virtual_hash = self._hash(virtual_name)
            self.shards_to_idx[virtual_name] = virtual_hash
            self.virtual_to_physical[virtual_name] = node_name

        self.data_store.create_node(node_name)
        self.visualize_ring()
        self._rebalance_data_on_node_addition(node_name)

    def remove_node(self, node_name: str):
        virtual_nodes = [v for v, p in self.virtual_to_physical.items() if p == node_name]
        if not virtual_nodes:
            raise ValueError(f"Node {node_name} does not exist.")
        
        if len(set(self.virtual_to_physical.values())) == 1:
            raise ValueError("Cannot remove the last node in the cluster.")

        for v_node in virtual_nodes:
            del self.shards_to_idx[v_node]
            del self.virtual_to_physical[v_node]

        self._rebalance_data_on_node_removal(node_name)
        self.data_store.delete_node(node_name)
        self.visualize_ring()

    def visualize_ring(self):
        self.viz.visualize_ring(self.shards_to_idx)

    def visualize_distribution(self):
        self.viz.visualize_distribution(self.shards_to_idx, self.data_store)
