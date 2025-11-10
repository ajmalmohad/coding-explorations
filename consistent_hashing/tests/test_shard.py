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

def test_add_node_creates_shard():
    m = ShardManager()
    m.add_node("NodeA")
    
    # Check physical node exists in virtual_to_physical mapping
    physical_nodes = set(m.virtual_to_physical.values())
    assert "NodeA" in physical_nodes
    assert os.path.exists("NodeA.csv")
    
    # Verify virtual nodes were created
    virtual_nodes = [v for v, p in m.virtual_to_physical.items() if p == "NodeA"]
    assert len(virtual_nodes) == m.virtual_nodes

def test_add_duplicate_node_raises_error():
    m = ShardManager()
    m.add_node("NodeA")
    
    with pytest.raises(ValueError, match="already exists"):
        m.add_node("NodeA")

def test_remove_node_deletes_shard():
    m = ShardManager()
    m.add_node("NodeA")
    m.add_node("NodeB")
    
    assert os.path.exists("NodeA.csv")
    m.remove_node("NodeA")
    
    # Check no virtual nodes remain for NodeA
    assert "NodeA" not in m.virtual_to_physical.values()
    assert not os.path.exists("NodeA.csv")

def test_remove_nonexistent_node_raises_error():
    m = ShardManager()
    m.add_node("NodeA")
    
    with pytest.raises(ValueError, match="does not exist"):
        m.remove_node("NodeB")

def test_remove_last_node_raises_error():
    m = ShardManager()
    m.add_node("NodeA")
    
    with pytest.raises(ValueError, match="Cannot remove the last node"):
        m.remove_node("NodeA")

def test_insert_and_get_data():
    m = ShardManager()
    m.add_node("NodeA")
    
    m.insert_data(["id1", "payload1", "2025-01-01"])
    res = m.get_data("id1")
    
    assert len(res) == 1
    assert res.iloc[0]['id'] == "id1"
    assert res.iloc[0]['data'] == "payload1"
    assert res.iloc[0]['created_at'] == "2025-01-01"

def test_delete_data():
    m = ShardManager()
    m.add_node("NodeA")
    
    m.insert_data(["id1", "payload1", "2025-01-01"])
    m.delete_data("id1")
    res = m.get_data("id1")
    
    assert len(res) == 0

def test_insert_multiple_records_same_id():
    m = ShardManager()
    m.add_node("NodeA")
    
    m.insert_data(["id1", "payload1", "2025-01-01"])
    m.insert_data(["id1", "payload2", "2025-01-02"])
    res = m.get_data("id1")
    
    assert len(res) == 2

def test_get_nonexistent_data():
    m = ShardManager()
    m.add_node("NodeA")
    
    res = m.get_data("nonexistent")
    assert len(res) == 0

def test_delete_nonexistent_data():
    m = ShardManager()
    m.add_node("NodeA")
    m.insert_data(["id1", "payload1", "2025-01-01"])
    
    m.delete_data("nonexistent")
    res = m.get_data("id1")
    
    assert len(res) == 1

def test_operations_on_empty_cluster():
    m = ShardManager()
    
    with pytest.raises(ValueError, match="No nodes available"):
        m.get_data("id1")
    
    with pytest.raises(ValueError, match="No nodes available"):
        m.insert_data(["id1", "payload1", "2025-01-01"])
    
    with pytest.raises(ValueError, match="No nodes available"):
        m.delete_data("id1")

def test_hash_consistency():
    m1 = ShardManager()
    m2 = ShardManager()
    
    h1 = m1._hash("test_key")
    h2 = m2._hash("test_key")
    
    assert h1 == h2

def test_hash_within_bounds():
    m = ShardManager()
    
    for i in range(100):
        h = m._hash(f"key{i}")
        assert 0 <= h < m.max_limit

def test_hash_distribution():
    m = ShardManager()
    
    hashes = {m._hash(f"key{i}") for i in range(100)}
    assert len(hashes) > 90  # Should have mostly unique hashes

def test_data_distributes_across_nodes():
    m = ShardManager()
    m.add_node("NodeA")
    m.add_node("NodeB")
    m.add_node("NodeC")
    
    ids = [f"id{i}" for i in range(50)]
    for k in ids:
        m.insert_data([k, f"payload-{k}", "2025-01-01"])
    
    # Get physical node names
    physical_nodes = set(m.virtual_to_physical.values())
    
    # Verify all data is stored
    all_stored_ids = set()
    for node in physical_nodes:
        all_stored_ids.update(read_ids_for_node(node))
    
    assert set(ids) == all_stored_ids
    
    # Verify data is distributed (at least 2 nodes should have data)
    nodes_with_data = sum(1 for node in physical_nodes if len(read_ids_for_node(node)) > 0)
    assert nodes_with_data >= 2

def test_rebalance_on_node_addition():
    m = ShardManager()
    m.add_node("NodeA")
    
    ids = [f"id{i}" for i in range(30)]
    for k in ids:
        m.insert_data([k, f"payload-{k}", "2025-01-01"])
    
    # All data should be on NodeA
    assert set(ids) == set(read_ids_for_node("NodeA"))
    
    m.add_node("NodeB")
    
    # Data should be redistributed
    physical_nodes = set(m.virtual_to_physical.values())
    all_stored_ids = set()
    for node in physical_nodes:
        node_ids = read_ids_for_node(node)
        all_stored_ids.update(node_ids)
        
        # Each stored ID should map to this node
        for k in node_ids:
            assert m._find_node_for_hash(m._hash(k)) == node
    
    assert set(ids) == all_stored_ids

def test_rebalance_on_node_removal():
    m = ShardManager()
    m.add_node("NodeA")
    m.add_node("NodeB")
    m.add_node("NodeC")
    
    ids = [f"id{i}" for i in range(40)]
    for k in ids:
        m.insert_data([k, f"payload-{k}", "2025-01-01"])
    
    m.remove_node("NodeB")
    
    # All data should still exist on remaining nodes
    physical_nodes = set(m.virtual_to_physical.values())
    all_stored_ids = set()
    for node in physical_nodes:
        all_stored_ids.update(read_ids_for_node(node))
    
    assert set(ids) == all_stored_ids
    assert "NodeB" not in physical_nodes

def test_multiple_rebalancing_operations():
    m = ShardManager()
    m.add_node("Node1")
    
    ids = [f"id{i}" for i in range(30)]
    for k in ids:
        m.insert_data([k, f"payload-{k}", "2025-01-01"])
    
    # Add nodes
    m.add_node("Node2")
    m.add_node("Node3")
    m.add_node("Node4")
    
    # Remove some nodes
    m.remove_node("Node2")
    m.remove_node("Node4")
    
    # Verify all data still exists
    physical_nodes = set(m.virtual_to_physical.values())
    all_stored_ids = set()
    for node in physical_nodes:
        all_stored_ids.update(read_ids_for_node(node))
    
    assert set(ids) == all_stored_ids
    assert len(physical_nodes) == 2

def test_consistent_mapping_after_rebalance():
    m = ShardManager()
    m.add_node("NodeA")
    m.add_node("NodeB")
    m.add_node("NodeC")
    
    ids = [f"id{i}" for i in range(50)]
    for k in ids:
        m.insert_data([k, f"payload-{k}", "2025-01-01"])
    
    # Verify each ID is on its mapped node
    for k in ids:
        expected_node = m._find_node_for_hash(m._hash(k))
        stored_ids = set(read_ids_for_node(expected_node))
        assert k in stored_ids, f"{k} should be on {expected_node}"
