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

    ids_in_a = set(read_ids_for_node("NodeA"))
    assert set(ids) == ids_in_a

    m.add_node("NodeB")
    # After rebalance, each id should be stored on the node that maps to it
    for k in ids:
        expected_node = m._find_closest_next_node_for_hash(m._hash(k))
        stored_ids = set(read_ids_for_node(expected_node))
        assert k in stored_ids, f"{k} not found in expected node {expected_node}"

def test_rebalance_on_removal_moves_items_and_deletes_file():
    m = ShardManager()
    m.add_node("NodeA")
    m.add_node("NodeB")
    m.add_node("NodeC")

    ids = [f"x{i}" for i in range(20)]
    for k in ids:
        m.insert_data([k, f"v-{k}", "2025-01-01"])

    assert os.path.exists("NodeB.csv")
    m.remove_node("NodeB")
    assert not os.path.exists("NodeB.csv")

    for k in ids:
        expected = m._find_closest_next_node_for_hash(m._hash(k))
        stored_ids = set(read_ids_for_node(expected))
        assert k in stored_ids, f"{k} missing from {expected}"

def test_insert_with_invalid_data_length():
    m = ShardManager()
    m.add_node("NodeA")
    
    with pytest.raises(ValueError, match="Data length .* does not match schema length"):
        m.insert_data(["id1", "payload1"]) # Too few fields
    
    with pytest.raises(ValueError, match="Data length .* does not match schema length"):
        m.insert_data(["id1", "payload1", "2025-01-01", "extra"]) # Too many fields

def test_get_data_from_empty_cluster():
    m = ShardManager()
    
    with pytest.raises(ValueError, match="No nodes available in cluster"):
        m.get_data("some_id")

def test_insert_data_into_empty_cluster():
    m = ShardManager()
    
    with pytest.raises(ValueError, match="No nodes available in cluster"):
        m.insert_data(["id1", "payload1", "2025-01-01"])

def test_delete_data_from_empty_cluster():
    m = ShardManager()
    
    with pytest.raises(ValueError, match="No nodes available in cluster"):
        m.delete_data("id1")

def test_get_nonexistent_id():
    m = ShardManager()
    m.add_node("NodeA")
    
    res = m.get_data("nonexistent_id")
    assert len(res) == 0

def test_delete_nonexistent_id():
    m = ShardManager()
    m.add_node("NodeA")
    m.insert_data(["id1", "payload1", "2025-01-01"])
    
    # Delete non-existent ID should not raise, just no-op
    m.delete_data("nonexistent_id")
    
    # Original data should still exist
    res = m.get_data("id1")
    assert len(res) == 1

def test_node_name_with_csv_extension():
    m = ShardManager()
    m.add_node("NodeA.csv")
    
    assert os.path.exists("NodeA.csv")
    assert "NodeA.csv" in m.shards_to_idx
    
    m.insert_data(["id1", "data1", "2025-01-01"])
    res = m.get_data("id1")
    assert len(res) == 1

def test_multiple_inserts_same_id_on_same_node():
    m = ShardManager()
    m.add_node("NodeA")
    
    m.insert_data(["id1", "payload1", "2025-01-01"])
    m.insert_data(["id1", "payload2", "2025-01-02"])
    
    res = m.get_data("id1")
    # Both entries should exist (no deduplication)
    assert len(res) == 2

def test_hash_consistency():
    m1 = ShardManager()
    m2 = ShardManager()
    
    # Same input should produce same hash
    h1 = m1._hash("test_key")
    h2 = m2._hash("test_key")
    assert h1 == h2
    
    # Hash should be within bounds
    assert 0 <= h1 < m1.max_limit

def test_hash_modulo_limit():
    m = ShardManager()
    
    # All hashes should be within max_limit
    for i in range(100):
        h = m._hash(f"key{i}")
        assert 0 <= h < m.max_limit

def test_visualize_ring_empty():
    m = ShardManager()
    # Should not raise, just log empty ring
    m.visualize_ring()

def test_visualize_ring_with_nodes():
    m = ShardManager()
    m.add_node("NodeA")
    m.add_node("NodeB")
    # Should not raise
    m.visualize_ring()

def test_visualize_distribution_empty():
    m = ShardManager()
    # Should not raise
    m.visualize_distribution()

def test_visualize_distribution_with_data():
    m = ShardManager()
    m.add_node("NodeA")
    m.add_node("NodeB")
    
    for i in range(10):
        m.insert_data([f"id{i}", f"data{i}", "2025-01-01"])
    
    # Should not raise
    m.visualize_distribution()

def test_read_all_data_missing_file():
    m = ShardManager()
    
    # Reading from non-existent node should return empty DataFrame
    df = m.data_store.get_all_data("NonExistentNode")
    assert len(df) == 0
    assert list(df.columns) == ["id", "data", "created_at"]

def test_rebalance_with_hash_collision_boundary():
    m = ShardManager()
    m.add_node("NodeA")
    m.add_node("NodeB")
    
    # Insert items and verify no data loss during rebalance
    ids = [f"test{i}" for i in range(50)]
    for k in ids:
        m.insert_data([k, f"v-{k}", "2025-01-01"])
    
    # Add third node
    m.add_node("NodeC")
    
    # Verify all data still exists
    all_stored_ids = set()
    for node in m.shards_to_idx.keys():
        all_stored_ids.update(read_ids_for_node(node))
    
    assert set(ids) == all_stored_ids

def test_remove_middle_node_redistributes_correctly():
    m = ShardManager()
    m.add_node("NodeA")
    m.add_node("NodeB")
    m.add_node("NodeC")
    
    ids = [f"mid{i}" for i in range(30)]
    for k in ids:
        m.insert_data([k, f"v-{k}", "2025-01-01"])
    
    # Remove middle node (by hash position)
    m.remove_node("NodeB")
    
    # Verify all data still accessible
    all_stored_ids = set()
    for node in m.shards_to_idx.keys():
        all_stored_ids.update(read_ids_for_node(node))
    
    assert set(ids) == all_stored_ids
    assert len(m.shards_to_idx) == 2

def test_sequential_add_remove_stability():
    m = ShardManager()
    m.add_node("Node1")
    
    ids = [f"stable{i}" for i in range(10)]
    for k in ids:
        m.insert_data([k, f"v-{k}", "2025-01-01"])
    
    # Add and remove nodes
    m.add_node("Node2")
    m.add_node("Node3")
    m.remove_node("Node2")
    
    # Verify data integrity
    all_stored_ids = set()
    for node in m.shards_to_idx.keys():
        all_stored_ids.update(read_ids_for_node(node))
    
    assert set(ids) == all_stored_ids

def test_idx_to_shards_consistency():
    m = ShardManager()
    m.add_node("NodeA")
    m.add_node("NodeB")
    
    # Verify bidirectional mapping consistency
    for node_name, hash_val in m.shards_to_idx.items():
        assert m.idx_to_shards[hash_val] == node_name
    
    for hash_val, node_name in m.idx_to_shards.items():
        assert m.shards_to_idx[node_name] == hash_val
