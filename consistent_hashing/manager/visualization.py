import logging

from consistent_hashing.manager.data_store import DataStore


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
