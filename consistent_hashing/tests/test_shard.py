import os
import pandas as pd
import pytest

from consistent_hashing.manager.shard_manager import ShardManager

@pytest.fixture(autouse=True)
def chdir_tmp_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    yield

def read_ids_for_node(node_name):
    fn = node_name if node_name.endswith(".csv") else f"{node_name}.csv"
    if not os.path.exists(fn):
        return []
    df = pd.read_csv(fn)
    return df['id'].astype(str).tolist()

def test_add_insert_get_delete_flow():
    m = ShardManager()
    m.add_node("NodeA")
    assert os.path.exists("NodeA.csv")
    m.insert_data(["id1", "payload1", "2025-01-01"])
    res = m.get_data("id1")
    # get_data returns DataFrame-like result
    assert len(res) == 1
    assert res.iloc[0]['id'] == "id1"

    m.delete_data("id1")
    res2 = m.get_data("id1")
    assert len(res2) == 0

def test_add_duplicate_and_remove_errors():
    m = ShardManager()
    m.add_node("NodeA")
    with pytest.raises(ValueError):
        m.add_node("NodeA")  # duplicate

    with pytest.raises(ValueError):
        m.remove_node("NodeX")  # non-existent

    # removing last node should raise
    with pytest.raises(ValueError):
        m.remove_node("NodeA")

def test_rebalance_on_addition_moves_items():
    m = ShardManager()
    m.add_node("NodeA")
    ids = [f"k{i}" for i in range(10)]
    for k in ids:
        m.insert_data([k, f"v-{k}", "2025-01-01"])

    # before adding second node all ids in NodeA.csv
    ids_in_a = set(read_ids_for_node("NodeA"))
    assert set(ids) == ids_in_a

    m.add_node("NodeB")

    # After rebalance, each id should be stored on the node that maps to it
    for k in ids:
        expected_node = m._find_closest_next_node_for_key(k)
        stored_ids = set(read_ids_for_node(expected_node))
        assert k in stored_ids, f"{k} not found in expected node {expected_node}"

def test_rebalance_on_removal_moves_items_and_deletes_file():
    m = ShardManager()
    # create three nodes
    m.add_node("NodeA")
    m.add_node("NodeB")
    m.add_node("NodeC")

    ids = [f"x{i}" for i in range(20)]
    for k in ids:
        m.insert_data([k, f"v-{k}", "2025-01-01"])

    # ensure files exist
    assert os.path.exists("NodeB.csv")

    # remove NodeB and ensure its items moved and file removed
    m.remove_node("NodeB")
    assert not os.path.exists("NodeB.csv")

    for k in ids:
        expected = m._find_closest_next_node_for_key(k)
        stored_ids = set(read_ids_for_node(expected))
        assert k in stored_ids, f"{k} missing from {expected}"
