import logging
import os
import pandas as pd


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

    def get_all(self, node_name: str) -> pd.DataFrame:
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
        if len(data) != len(schema):
            raise ValueError(f"Data length {len(data)} does not match schema length {len(schema)}.")

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
