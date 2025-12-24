from engine.engine import InnoEngine

def main():
    engine = InnoEngine()
    
    print("=== Inserting 24 rows ===\n")
    data = [
        (1, "Bob"), (2, "Carol"), (3, "Dave"), (4, "Eve"), (5, "Frank"), 
        (6, "George"), (7, "Harry"), (8, "Ivy"), (9, "Jack"), (10, "Lily"), 
        (11, "Mason"), (12, "Nathan"), (13, "Olivia"), (14, "Paul"), 
        (15, "Quincy"), (16, "Ryan"), (17, "Sarah"), (18, "Thomas"), 
        (19, "Uma"), (20, "Victoria"), (21, "William"), (22, "Xavier"), 
        (23, "Yara"), (24, "Zara")
    ]
    
    for row in data:
        engine.insert_row(row)
    
    print(engine.index.pretty_print())
    #Test retrieving specific rows
    print("\n=== Testing row retrieval ===")
    test_rows = [1, 5, 10, 15, 20, 24]
    for row_id in test_rows:
        row = engine.get_row(row_id)
        print(f"Row {row_id}: {row}")
    
    # Check B+Tree structure
    print("\n=== B+Tree Structure ===")
    engine.index.pretty_print()
    
    # Show all mappings
    print("\n=== All row_id → page_id mappings ===")
    mappings = engine.index.traverse()
    for row_id, page_id in mappings:
        print(f"  Row {row_id} → Page {page_id}")
    
    # Shutdown and save
    print()
    engine.shutdown()

    print()
    # Print stats
    engine.print_stats()

if __name__ == "__main__":
    main()