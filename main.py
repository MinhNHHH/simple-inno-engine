from engine.engine import InnoEngine
from memory.pages import Page

def main():
    engine = InnoEngine()
    
    data = [(1, "Bob"), (2, "Carol"), (3, "Dave"), (4, "Eve"), (5, "Frank"), 
            (6, "George"), (7, "Harry"), (8, "Ivy"), (9, "Jack"), (10, "Lily"), 
            (11, "Mason"), (12, "Nathan"), (13, "Olivia"), (14, "Paul"), 
            (15, "Quincy"), (16, "Ryan"), (17, "Sarah"), (18, "Thomas"), 
            (19, "Uma"), (20, "Victoria"), (21, "William"), (22, "Xavier"), 
            (23, "Yara"), (24, "Zara")]
    
    for row in data:
        engine.insert_page(row)
    
    # # ✅ Test retrieving specific rows
    # print("\n=== Testing row retrieval ===")
    # print("Row 1:", engine.get_row(1))
    # print("Row 2:", engine.get_row(2))
    # print("Row 10:", engine.get_row(10))
    
    # # ✅ Check which pages exist
    # print("\n=== Pages on disk ===")
    # print(f"Total pages: {len(engine.disk.pages)}")
    # print(f"Page IDs: {sorted(engine.disk.pages.keys())}")
    
    # ✅ Load a specific page that exists
    print("\n=== Page 1 contents ===")
    print(engine.buffer_pool.load_page(1).rows)
    engine.buffer_pool.release_page(1)
    print("\n=== Page 0 contents ===")
    print(engine.buffer_pool.load_page(2).rows)
    engine.buffer_pool.release_page(2)

if __name__ == "__main__":
    main()