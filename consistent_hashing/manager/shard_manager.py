import bisect
import hashlib
import logging

from consistent_hashing.manager.data_store import DataStore
from consistent_hashing.manager.visualization import Visualization


class ShardManager:
    def __init__(self, max_limit=2**32, virtual_nodes=150):
        self.shards_to_idx: dict[str, int] = {}
        self.max_limit = max_limit
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
        return sorted_items[idx % len(sorted_items)][0]

    def get_data(self, id: str):
        node_name = self._find_node_for_hash(self._hash(id))
        logging.debug("get_data: id=%s mapped to node=%s", id, node_name)
        return self.data_store.get_by_id(id, node_name)

    def insert_data(self, data: list[str]):
        id = data[0]
        node_name = self._find_node_for_hash(self._hash(id))
        logging.debug("insert_data: id=%s will go to node=%s", id, node_name)
        self.data_store.insert(data, node_name)

    def delete_data(self, id: str):
        node_name = self._find_node_for_hash(self._hash(id))
        self.data_store.delete_by_id(id, node_name)

    def _rebalance_data_on_node_addition(self, node_name):
        new_node_hash = self._hash(node_name)
        next_node = self._find_node_for_hash((new_node_hash + 1) % self.max_limit)
        
        logging.info("Rebalancing: new node %s (hash=%s) will pull from %s", 
                     node_name, new_node_hash, next_node)
        
        all_data = self.data_store.get_all(next_node)
        for item in all_data.values.tolist():
            item_node = self._find_node_for_hash(self._hash(item[0]))
            if item_node != next_node:
                self.insert_data(item)
                self.data_store.delete_by_id(item[0], next_node)

    def _rebalance_data_on_node_removal(self, node_name):
        all_data = self.data_store.get_all(node_name)
        next_node = self._find_node_for_hash((self._hash(node_name) + 1) % self.max_limit)
        
        for item in all_data.values.tolist():
            self.data_store.insert(item, next_node)
            self.data_store.delete_by_id(item[0], node_name)

    def add_node(self, node_name: str):
        if node_name in self.shards_to_idx:
            raise ValueError(f"Node {node_name} already exists.")
        
        node_hash = self._hash(node_name)
        self.shards_to_idx[node_name] = node_hash
        logging.info("Node %s added with hash %s. Current ring:", node_name, node_hash)
        self.data_store.create_node(node_name)
        self.visualize_ring()
        self._rebalance_data_on_node_addition(node_name)

    def remove_node(self, node_name: str):
        if node_name not in self.shards_to_idx:
            raise ValueError(f"Node {node_name} does not exist.")
        
        if len(self.shards_to_idx) == 1:
            raise ValueError("Cannot remove the last node in the cluster.")

        self._rebalance_data_on_node_removal(node_name)
        del self.shards_to_idx[node_name]
        self.data_store.delete_node(node_name)

        logging.info("Node %s removed. Current ring:", node_name)
        self.visualize_ring()

    def visualize_ring(self):
        self.viz.visualize_ring(self.shards_to_idx)

    def visualize_distribution(self):
        self.viz.visualize_distribution(self.shards_to_idx, self.data_store)
