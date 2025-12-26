"""
Transaction Demo for InnoDB Engine
Demonstrates ACID properties:
- Atomicity: All-or-nothing execution
- Consistency: Valid state transitions
- Isolation: Row-level locking
- Durability: Write-ahead logging
"""

from engine.engine import InnoEngine
from memory.index import BPlusTree

def demo_basic_transaction():
    """Demo: Basic transaction with commit"""
    print("=" * 70)
    print("DEMO 1: Basic Transaction with COMMIT")
    print("=" * 70)
    index = BPlusTree(t=3)
    engine = InnoEngine(index=index)
    
    # Begin transaction
    tx1 = engine.begin_transaction()
    
    # Insert rows within transaction
    engine.tx_insert_row(tx1, (1, "Alice", 30))
    engine.tx_insert_row(tx1, (2, "Bob", 25))
    engine.tx_insert_row(tx1, (3, "Charlie", 35))
    
    # Commit transaction
    tx1.commit()
    
    # Verify rows are persisted
    print("\n--- Verifying inserted rows ---")
    for row_id in [1, 2, 3]:
        row = engine.get_row(row_id)
        print(f"Row {row_id}: {row}")
    
    print("\n✓ Transaction committed successfully\n")
    return engine


def demo_rollback_transaction():
    """Demo: Transaction rollback (Atomicity)"""
    print("=" * 70)
    print("DEMO 2: Transaction ROLLBACK (Atomicity)")
    print("=" * 70)
    
    index = BPlusTree(t=3)
    engine = InnoEngine(index=index)
    
    # Insert initial data
    tx1 = engine.begin_transaction()
    engine.tx_insert_row(tx1, (1, "Alice", 30))
    engine.tx_insert_row(tx1, (2, "Bob", 25))
    tx1.commit()
    
    print("\n--- Initial state ---")
    for row_id in [1, 2]:
        row = engine.get_row(row_id)
        print(f"Row {row_id}: {row}")
    
    # Start a new transaction and make changes
    tx2 = engine.begin_transaction()
    engine.tx_update_row(tx2, 1, (1, "Alice", 31))  # Update Alice's age
    engine.tx_insert_row(tx2, (3, "Charlie", 35))   # Insert Charlie
    engine.tx_delete_row(tx2, 2)                     # Delete Bob
    
    print("\n--- Before rollback (changes in transaction) ---")
    print("Row 1:", engine.get_row(1))
    try:
        print("Row 2:", engine.get_row(2))
    except Exception as e:
        print(f"Row 2: {e}")
    print("Row 3:", engine.get_row(3))
    
    # Rollback the transaction
    print("\n--- Rolling back transaction ---")
    tx2.rollback()
    
    print("\n--- After rollback (restored to original state) ---")
    for row_id in [1, 2]:
        row = engine.get_row(row_id)
        print(f"Row {row_id}: {row}")
    
    try:
        engine.get_row(3)
        print("Row 3: Found (should not exist)")
    except Exception:
        print("Row 3: Not found (correct - rollback successful)")
    
    print("\n✓ Rollback successful - Atomicity demonstrated\n")
    return engine


def demo_isolation():
    """Demo: Transaction isolation with locking"""
    print("=" * 70)
    print("DEMO 3: Transaction ISOLATION (Row-level Locking)")
    print("=" * 70)
    
    index = BPlusTree(t=3)
    engine = InnoEngine(index=index)
    
    # Insert initial data
    tx0 = engine.begin_transaction()
    engine.tx_insert_row(tx0, (1, "Alice", 30))
    engine.tx_insert_row(tx0, (2, "Bob", 25))
    tx0.commit()
    
    print("\n--- Initial state ---")
    for row_id in [1, 2]:
        row = engine.get_row(row_id)
        print(f"Row {row_id}: {row}")
    
    # Transaction 1: Update row 1
    print("\n--- Transaction 1: Acquiring lock on row 1 ---")
    tx1 = engine.begin_transaction()
    engine.tx_update_row(tx1, 1, (1, "Alice", 31))
    
    # Transaction 2: Try to update row 1 (should fail - locked by tx1)
    print("\n--- Transaction 2: Trying to acquire lock on row 1 (will fail) ---")
    tx2 = engine.begin_transaction()
    try:
        engine.tx_update_row(tx2, 1, (1, "Alice", 32))  # Should fail
        print("ERROR: Should not have acquired lock!")
    except Exception as e:
        print(f"✓ Lock conflict detected: {e}")
    
    # Transaction 2: Can update row 2 (different row)
    print("\n--- Transaction 2: Acquiring lock on row 2 (different row) ---")
    engine.tx_update_row(tx2, 2, (2, "Bob", 26))
    print("✓ Lock acquired on row 2 (no conflict)")
    
    # Commit tx1, releasing lock on row 1
    print("\n--- Transaction 1: Committing (releases lock on row 1) ---")
    tx1.commit()
    
    # Now tx2 can be committed
    print("\n--- Transaction 2: Committing ---")
    tx2.commit()
    
    print("\n--- Final state ---")
    for row_id in [1, 2]:
        row = engine.get_row(row_id)
        print(f"Row {row_id}: {row}")
    
    print("\n✓ Isolation demonstrated - Locks prevent conflicts\n")
    return engine


def demo_durability():
    """Demo: Durability with Write-Ahead Logging"""
    print("=" * 70)
    print("DEMO 4: DURABILITY (Write-Ahead Logging)")
    print("=" * 70)
    
    index = BPlusTree(t=3)
    engine = InnoEngine(index=index)
    
    # Transaction with multiple operations
    tx1 = engine.begin_transaction()
    engine.tx_insert_row(tx1, (1, "Alice", 30))
    engine.tx_insert_row(tx1, (2, "Bob", 25))
    engine.tx_insert_row(tx1, (3, "Charlie", 35))
    
    print(f"\n--- Redo log entries before commit ---")
    print(f"Number of redo log records: {len(engine.redo_record.records)}")
    for record in engine.redo_record.records:
        print("1=====================",record)
        print(f"  LSN {record.lsn}: {record.action} on page {record.page_id}")
    
    # Commit flushes redo log (WAL)
    tx1.commit()
    
    print(f"\n--- After commit (redo log flushed) ---")
    print(f"Flushed LSN: {engine.redo_record.flushed_lsn}")
    
    # Save to disk
    engine.redo_record.dump_to_json("redo_log.json")
    print("✓ Redo log saved to redo_log.json")
    
    print("\n✓ Durability demonstrated - Changes logged before commit\n")
    return engine


def demo_complex_transaction():
    """Demo: Complex transaction with multiple operations"""
    print("=" * 70)
    print("DEMO 5: Complex Transaction (INSERT, UPDATE, DELETE)")
    print("=" * 70)
    
    index = BPlusTree(t=3)
    engine = InnoEngine(index=index)
    
    # Setup initial data
    print("--- Setting up initial data ---")
    tx0 = engine.begin_transaction()
    engine.tx_insert_row(tx0, (1, "Alice", 30))
    engine.tx_insert_row(tx0, (2, "Bob", 25))
    engine.tx_insert_row(tx0, (3, "Charlie", 35))
    tx0.commit()
    
    print("Initial rows:")
    for row_id in [1, 2, 3]:
        print(f"  {engine.get_row(row_id)}")
    
    # Complex transaction
    print("\n--- Starting complex transaction ---")
    tx1 = engine.begin_transaction()
    
    # Update Alice
    engine.tx_update_row(tx1, 1, (1, "Alice Smith", 31))
    
    # Delete Bob
    engine.tx_delete_row(tx1, 2)
    
    # Insert new rows
    engine.tx_insert_row(tx1, (4, "David", 28))
    engine.tx_insert_row(tx1, (5, "Eve", 22))
    
    # Update Charlie
    engine.tx_update_row(tx1, 3, (3, "Charlie Brown", 36))
    
    print(f"\nTransaction {tx1.txid} statistics:")
    print(f"  - Undo records: {len(tx1.undo_record.records)}")
    print(f"  - Redo LSNs: {len(tx1.redo_record.records)}")
    print(f"  - Locked rows: {tx1.locked_rows}")
    
    # Commit
    tx1.commit()
    
    print("\n--- Final state after commit ---")
    for row_id in [1, 3, 4, 5]:
        print(f"  {engine.get_row(row_id)}")
    
    try:
        engine.get_row(2)
        print("  Row 2: Found (should be deleted)")
    except Exception:
        print("  Row 2: Deleted (correct)")
    
    print("\n✓ Complex transaction completed successfully\n")
    
    # Show database stats
    engine.print_stats()
    
    return engine


def main():
    """Run all demos"""
    print("\n")
    print("╔" + "=" * 68 + "╗")
    print("║" + " " * 15 + "INNODB TRANSACTION SYSTEM DEMO" + " " * 23 + "║")
    print("║" + " " * 20 + "ACID Properties Demonstration" + " " * 19 + "║")
    print("╚" + "=" * 68 + "╝")
    print("\n")
    
    # Run demos
    demo_basic_transaction()
    input("Press Enter to continue to next demo...")
    
    demo_rollback_transaction()
    input("Press Enter to continue to next demo...")
    
    demo_isolation()
    input("Press Enter to continue to next demo...")
    
    demo_durability()
    input("Press Enter to continue to next demo...")
    
    demo_complex_transaction()
    
    print("\n" + "=" * 70)
    print("All demos completed successfully!")
    print("=" * 70)
    print("\nKey takeaways:")
    print("  ✓ Atomicity: Transactions are all-or-nothing (rollback demo)")
    print("  ✓ Consistency: Database maintains valid states")
    print("  ✓ Isolation: Row-level locks prevent conflicts")
    print("  ✓ Durability: Write-ahead logging ensures persistence")
    print("\n")


if __name__ == "__main__":
    main()
