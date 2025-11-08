import bisect
import hashlib
import logging
import os
import pandas as pd
from pandas import DataFrame

schema = ["id", "data", "created_at"]

class ShardManager:
    def __init__(self):
        self.shards_to_idx = {}
        self.idx_to_shards = {}
        self.max_limit = 10000
        self.logger = logging.getLogger(__name__)

    def _hash(self, data: str) -> int:
        h = hashlib.sha256(data.encode("utf-8")).hexdigest()
        return int(h, 16) % self.max_limit
    
    def _get_node_config(self, node_name: str) -> str:
        return node_name if node_name.endswith(".csv") else f"{node_name}.csv"
    
    def _create_node(self, node_name: str):
        file_path = self._get_node_config(node_name)
        df = pd.DataFrame(columns=schema)
        df.to_csv(file_path, index=False)
        self.logger.info("Created node file %s", file_path)

    def _delete_node(self, node_name: str):
        file_path = self._get_node_config(node_name)
        try:
            os.remove(file_path)
            self.logger.info("Deleted node file %s", file_path)
        except FileNotFoundError:
            self.logger.warning("Node file %s not found when attempting delete", file_path)

    def _find_closest_next_node_for_hash(self, hash: int):
        if not self.shards_to_idx:
            raise ValueError("No nodes available in cluster")

        all_indices = sorted(self.shards_to_idx.values())
        nearest_node_idx = bisect.bisect_left(all_indices, hash)
        if nearest_node_idx == len(all_indices):
            nearest_node_idx = 0
        nearest_node_hash = all_indices[nearest_node_idx]

        nearest_node = self.idx_to_shards[nearest_node_hash]
        self.logger.debug("Hash %s -> nearest node %s (hash %s)", hash, nearest_node, nearest_node_hash)
        return nearest_node

    def _find_closest_next_node_for_key(self, query_key: str):
        query_hash = self._hash(query_key)
        self.logger.debug("Key '%s' hashed to %s", query_key, query_hash)
        return self._find_closest_next_node_for_hash(query_hash)

    def _read_all_data(self, node_name: str) -> DataFrame:
        file_path = self._get_node_config(node_name)
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            self.logger.warning("Reading data from missing node file %s; returning empty DataFrame", file_path)
            df = pd.DataFrame(columns=schema)
        return df

    def _get_by_id_from_node(self, id: str, node_name: str):
        file_path = self._get_node_config(node_name)
        df = pd.read_csv(file_path)
        result = df[df['id'] == id]
        self.logger.info("Lookup id=%s on node=%s returned %d rows", id, node_name, len(result))
        return result
    
    def _insert_data_to_node(self, data: list[str], node_name: str):
        file_path = self._get_node_config(node_name)
        df = pd.read_csv(file_path)
        new_row = pd.DataFrame([data], columns=schema)
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(file_path, index=False)
        self.logger.info("Inserted id=%s into node=%s", data[0], node_name)

    def _delete_by_id_from_node(self, id: str, node_name: str):
        file_path = self._get_node_config(node_name)
        df = pd.read_csv(file_path)
        before = len(df)
        df = df[df['id'] != id]
        after = len(df)
        df.to_csv(file_path, index=False)
        self.logger.info("Deleted id=%s from node=%s (rows before=%d after=%d)", id, node_name, before, after)

    def get_data(self, id: str):
        node_name = self._find_closest_next_node_for_key(id)
        self.logger.debug("get_data: id=%s mapped to node=%s", id, node_name)
        return self._get_by_id_from_node(id, node_name)

    def insert_data(self, data: list[str]):
        if len(data) != len(schema):
            raise ValueError(f"Data length {len(data)} does not match schema length {len(schema)}.")
        
        id = data[0]
        node_name = self._find_closest_next_node_for_key(id)
        self.logger.debug("insert_data: id=%s will go to node=%s", id, node_name)
        self._insert_data_to_node(data, node_name)

    def delete_data(self, id: str):
        node_name = self._find_closest_next_node_for_key(id)
        self._delete_by_id_from_node(id, node_name)

    def _rebalance_data_on_node_addition(self, node_name):
        new_node_hash = self._hash(node_name)

        # find next existing node clockwise: items that used to belong to next_node may need moving
        next_node = self._find_closest_next_node_for_hash((new_node_hash + 1) % self.max_limit)
        self.logger.info("Rebalancing: new node %s (hash=%s) will pull from %s", node_name, new_node_hash, next_node)
        all_data = self._read_all_data(next_node)

        to_delete = []
        for item in all_data.values.tolist():
            item_node = self._find_closest_next_node_for_key(item[0])
            if item_node == next_node:
                continue
            else:
                # will insert to appropriate node (likely the new node)
                self.insert_data(item)
                to_delete.append(item[0])

        if to_delete:
            self.logger.info("Deleting %d rebalanced items from node %s", len(to_delete), next_node)

        for item in to_delete:
            self._delete_by_id_from_node(item, next_node)

    def _rebalance_data_on_node_removal(self, node_name):
        number_of_nodes = len(self.shards_to_idx)
        if number_of_nodes <= 1:
            raise ValueError(f"There are only {number_of_nodes} nodes present")

        all_data = self._read_all_data(node_name)
        current_node_hash = self._hash(node_name)

        # find next existing node clockwise: items needs to move there
        next_node = self._find_closest_next_node_for_hash((current_node_hash + 1) % self.max_limit)

        to_delete = []
        for item in all_data.values.tolist():
            self._insert_data_to_node(item, next_node)
            to_delete.append(item[0])

        if to_delete:
            self.logger.info("Deleting %d rebalanced items from node %s", len(to_delete), node_name)

        for item in to_delete:
            self._delete_by_id_from_node(item, node_name)

    def add_node(self, node_name: str):
        if node_name in self.shards_to_idx:
            raise ValueError(f"Node {node_name} already exists.")
        
        node_hash = self._hash(node_name)
        self.shards_to_idx[node_name] = node_hash
        self.idx_to_shards[node_hash] = node_name
        self.logger.info("Node %s added with hash %s. Current ring:", node_name, node_hash)
        self._create_node(node_name)
        self.visualize_ring()
        self._rebalance_data_on_node_addition(node_name)

    def remove_node(self, node_name: str):
        if node_name not in self.shards_to_idx:
            raise ValueError(f"Node {node_name} does not exist.")

        node_hash = self.shards_to_idx[node_name]
        self._rebalance_data_on_node_removal(node_name)
        del self.shards_to_idx[node_name]
        del self.idx_to_shards[node_hash]
        self._delete_node(node_name)

        self.logger.info("Node %s removed. Current ring:", node_name)
        self.visualize_ring()

    def visualize_ring(self):
        if not self.shards_to_idx:
            self.logger.info("Ring is empty.")
            return
        items = sorted((h, n) for n, h in self.shards_to_idx.items())
        lines = ["Ring (clockwise):"]
        for h, n in items:
            lines.append(f"  - hash={h:5d} -> node={n}")
        self.logger.info("\n" + "\n".join(lines))

    def visualize_distribution(self, show_samples: int = 3):
        if not self.shards_to_idx:
            self.logger.info("No nodes to show distribution for.")
            return
        lines = ["Distribution:"]
        for node in sorted(self.shards_to_idx):
            df = self._read_all_data(node)
            count = len(df)
            samples = df['id'].tolist()[:show_samples] if count else []
            lines.append(f"  - node={node} count={count} samples={samples}")
        self.logger.info("\n" + "\n".join(lines))

if __name__ == "__main__":
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.DEBUG,
    )
    manager = ShardManager()
    manager.add_node("NodeA")
    manager.insert_data(["hello", "world", "2024-10-01"])
    manager.add_node("NodeB")
    manager.insert_data(["hello2", "world2", "2024-10-01"])
    manager.add_node("NodeC")
    manager.visualize_distribution()

    manager.remove_node("NodeA")
    manager.remove_node("NodeB")
    manager.visualize_distribution()