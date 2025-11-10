import bisect
import hashlib
import logging
import os
import pandas as pd
from pandas import DataFrame

schema = ["id", "data", "created_at"]


class DataStore:
    def _get_node_config(self, node_name: str) -> str:
        return node_name if node_name.endswith(".csv") else f"{node_name}.csv"

    def create_node(self, node_name: str):
        file_path = self._get_node_config(node_name)
        df = pd.DataFrame(columns=schema)
        df.to_csv(file_path, index=False)
        logging.info("Created node file %s", file_path)

    def delete_node(self, node_name: str):
        file_path = self._get_node_config(node_name)
        try:
            os.remove(file_path)
            logging.info("Deleted node file %s", file_path)
        except FileNotFoundError:
            logging.warning("Node file %s not found when attempting delete", file_path)

    def get_all(self, node_name: str) -> DataFrame:
        file_path = self._get_node_config(node_name)
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            logging.warning("Reading data from missing node file %s; returning empty DataFrame", file_path)
            df = pd.DataFrame(columns=schema)
        return df

    def get_by_id(self, id: str, node_name: str):
        file_path = self._get_node_config(node_name)
        df = pd.read_csv(file_path)
        result = df[df['id'] == id]
        logging.info("Lookup id=%s on node=%s returned %d rows", id, node_name, len(result))
        return result

    def insert(self, data: list[str], node_name: str):
        file_path = self._get_node_config(node_name)
        df = pd.read_csv(file_path)
        new_row = pd.DataFrame([data], columns=schema)
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(file_path, index=False)
        logging.info("Inserted id=%s into node=%s", data[0], node_name)

    def delete_by_id(self, id: str, node_name: str):
        file_path = self._get_node_config(node_name)
        df = pd.read_csv(file_path)
        before = len(df)
        df = df[df['id'] != id]
        after = len(df)
        df.to_csv(file_path, index=False)
        logging.info("Deleted id=%s from node=%s (rows before=%d after=%d)", id, node_name, before, after)


class Visualization:
    def visualize_ring(self, shards_to_idx: dict[str, int]):
        if not shards_to_idx:
            logging.info("Ring is empty.")
            return
        items = sorted((h, n) for n, h in shards_to_idx.items())
        lines = ["Ring (clockwise):"]
        for h, n in items:
            lines.append(f"  - hash={h:5d} -> node={n}")
        logging.info("\n" + "\n".join(lines))

    def visualize_distribution(self, shards_to_idx: dict[str, int], data_store: DataStore, show_samples: int = 3):
        if not shards_to_idx:
            logging.info("No nodes to show distribution for.")
            return
        lines = ["Distribution:"]
        for node in sorted(shards_to_idx):
            df = data_store.get_all(node)
            count = len(df)
            samples = df['id'].tolist()[:show_samples] if count else []
            lines.append(f"  - node={node} count={count} samples={samples}")
        logging.info("\n" + "\n".join(lines))


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
        if len(data) != len(schema):
            raise ValueError(f"Data length {len(data)} does not match schema length {len(schema)}.")
        
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
    manager.remove_node("NodeC")
