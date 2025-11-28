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

    def delete_node(self, node_name: str):
        file_path = self._get_node_config(node_name)
        try:
            os.remove(file_path)
        except FileNotFoundError:
            return None

    def get_all(self, node_name: str) -> pd.DataFrame:
        file_path = self._get_node_config(node_name)
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            df = pd.DataFrame(columns=schema)
        return df

    def get_by_id(self, id: str, node_name: str):
        file_path = self._get_node_config(node_name)
        df = pd.read_csv(file_path)
        result = df[df['id'] == id]
        return result

    def insert(self, data: list[str], node_name: str):
        if len(data) != len(schema):
            raise ValueError(f"Data length {len(data)} does not match schema length {len(schema)}.")

        file_path = self._get_node_config(node_name)
        df = pd.read_csv(file_path)
        new_row = pd.DataFrame([data], columns=schema)
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(file_path, index=False)

    def delete_by_id(self, id: str, node_name: str):
        file_path = self._get_node_config(node_name)
        df = pd.read_csv(file_path)
        df = df[df['id'] != id]
        df.to_csv(file_path, index=False)
