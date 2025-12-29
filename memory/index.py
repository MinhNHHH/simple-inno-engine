# B+Tree implementation for InnoDB-style database indexing
# Maps row_id (key) -> page_id (value)

class BPlusTreeNode:
    def __init__(self, t, leaf=False):
        self.t = t  # minimum degree (defines the range for number of keys)
        self.keys: list[int] = []  # row_ids
        self.values: list[int] = []  # page_ids
        self.children: list['BPlusTreeNode'] = []  # child pointers
        self.leaf = leaf  # true when node is leaf
        self.next = None  # pointer to next leaf (for range scans)

    def is_full(self):
        """A node is full when number of keys == 2*t - 1"""
        return len(self.keys) == 2 * self.t - 1

    def find_key_index(self, k):
        """Return the index of the first key >= k"""
        idx = 0
        while idx < len(self.keys) and self.keys[idx] < k:
            idx += 1
        return idx


class BPlusTree:
    """
    B+Tree implementation for indexing rows in the database.
    Maps row_id -> page_id for efficient lookups.
    
    Key features:
    - All data (values) stored in leaf nodes only
    - Internal nodes contain only routing keys
    - Leaf nodes are linked for range scans
    - Supports insert, search, update operations
    """
    def __init__(self, t):
        if t < 2:
            raise ValueError("Minimum degree t must be at least 2")
        self.t = t
        self.root = BPlusTreeNode(t, leaf=True)

    def search(self, node: BPlusTreeNode, k: int) -> tuple[BPlusTreeNode, int]:
        i = node.find_key_index(k)
        if i < len(node.keys) and node.keys[i] == k:
            return node, i
        
        if node.leaf:
            return None, None
        
        return self.search(node.children[i], k)

    def get_page_id(self, row_id: int) -> int | None:
        node, idx = self.search(self.root, row_id)
        if node:
            return node.values[idx]
        return None

    def insert_row_mapping(self, row_id: int, page_id: int) -> None:
        """
        Insert or update the mapping from row_id to page_id.
        """
        # Check if row already exists
        node, idx = self.search(self.root, row_id)
        if node is not None:
            # Update existing mapping
            node.values[idx] = page_id
            return
        
        # Insert new mapping
        root = self.root
        
        # If root is full, tree grows in height
        if root.is_full():
            new_root = BPlusTreeNode(self.t, leaf=False)
            new_root.children.append(root)
            self.split_child(new_root, 0)
            self.root = new_root
            self._insert_non_full(new_root, row_id, page_id)
        else:
            self._insert_non_full(root, row_id, page_id)

    def update_page_id(self, row_id: int, new_page_id: int) -> None:
        node, idx = self.search(self.root, row_id)
        if node is None:
            raise KeyError(f"Row {row_id} not found in index")
        node.values[idx] = new_page_id
    
    def delete_row_mapping(self, row_id: int) -> None:
        node, idx = self.search(self.root, row_id)
        if node is not None and node.leaf:
s            del node.keys[idx]
            del node.values[idx]


    def split_child(self, parent: BPlusTreeNode, i: int) -> None:
        """
        Split the full child at parent.children[i] into two nodes.
        For B+Tree: in leaf nodes, copy the median key up (don't remove it).
        """
        t = self.t
        full_child = parent.children[i]
        new_child = BPlusTreeNode(t, leaf=full_child.leaf)
        median = t - 1
        
        if full_child.leaf:
            # For leaf nodes: copy median key up, keep it in left child
            promoted_key = full_child.keys[median]
            promoted_value = full_child.values[median]
            
            # Split keys and values
            new_child.keys = full_child.keys[median:]
            new_child.values = full_child.values[median:]
            
            full_child.keys = full_child.keys[:median]
            full_child.values = full_child.values[:median]
            
            # Link siblings
            new_child.next = full_child.next
            full_child.next = new_child
        else:
            # For internal nodes: push median key up, remove from child
            promoted_key = full_child.keys[median]
            promoted_value = full_child.values[median]

            new_child.keys = full_child.keys[median + 1:]
            new_child.values = full_child.values[median + 1:]
            new_child.children = full_child.children[median + 1:]
            
            full_child.keys = full_child.keys[:median]
            full_child.values = full_child.values[:median]
            full_child.children = full_child.children[:median + 1]
        
        # Insert new child and promoted key into parent
        parent.children.insert(i + 1, new_child)
        parent.keys.insert(i, promoted_key)
        parent.values.insert(i, promoted_value)

    def _insert_non_full(self, node: BPlusTreeNode, k: int, v: int) -> None:
        i = len(node.keys) - 1
        
        if node.leaf:
            node.keys.append(None)
            node.values.append(None)
            
            while i >= 0 and node.keys[i] > k:
                node.keys[i + 1] = node.keys[i]
                node.values[i + 1] = node.values[i]
                i -= 1
            
            node.keys[i + 1] = k
            node.values[i + 1] = v
        else:
            # Find child to insert into
            while i >= 0 and node.keys[i] > k:
                i -= 1
            i += 1
            
            # If the found child is full, split it
            if node.children[i].is_full():
                self.split_child(node, i)
                # After split, decide which of the two children to descend to
                if k > node.keys[i]:
                    i += 1
            
            self._insert_non_full(node.children[i], k, v)

    def traverse(self, node=None) -> list[tuple[int, int]]:
        if node is None:
            node = self.root
        
        result = []
        
        if node.leaf:
            for i in range(len(node.keys)):
                result.append((node.keys[i], node.values[i]))
        else:
            for i in range(len(node.keys)):
                result.extend(self.traverse(node.children[i]))
            # Don't forget the last child
            result.extend(self.traverse(node.children[len(node.keys)]))
        
        return result

    def traverse_leaves(self) -> list[tuple[int, int]]:
        node = self.root
        while not node.leaf:
            node = node.children[0]
        
        # Traverse leaf linked list
        result = []
        while node is not None:
            for i in range(len(node.keys)):
                result.append((node.keys[i], node.values[i]))
            node = node.next
        
        return result

    def pretty_print(self):
        """Print the tree level by level"""
        from collections import deque
        q = deque([(self.root, 0)])
        last_level = 0
        lines = []
        
        while q:
            node, lvl = q.popleft()
            if lvl != last_level:
                lines.append("\n")
                last_level = lvl
            
            if node.leaf:
                # Show keys and values for leaf nodes
                pairs = [f"{k}→{v}" for k, v in zip(node.keys, node.values)]
                lines.append(f"[{', '.join(pairs)}]  ")
            else:
                # Show only keys for internal nodes
                lines.append(f"{node.keys}  ")
                for c in node.children:
                    q.append((c, lvl + 1))
        
        print("".join(lines))
    
    def dump_to_json(self, filename):
        """Dump the B+Tree structure as JSON to a file."""
        import json

        def node_to_dict(node):
            d = {
                "keys": node.keys,
                "leaf": node.leaf,
                "values": node.values
            }
            if not node.leaf:
                d["children"] = [node_to_dict(child) for child in node.children]
            return d

        tree_dict = node_to_dict(self.root)
        with open(filename, "w") as f:
            json.dump(tree_dict, f, indent=4)
    
    @classmethod
    def load_from_json(cls, t: int=2) -> 'BPlusTree':
        """Load and restore a B+Tree structure from a JSON file."""
        import json

        def dict_to_node(d, t):
            node = BPlusTreeNode(t, leaf=d["leaf"])
            node.keys = d["keys"]
            node.values = d.get("values", [])  # Load values for both leaf and internal nodes
            if not d["leaf"]:
                node.children = [dict_to_node(child, t) for child in d["children"]]
            return node

        try:
            with open("index.json", "r") as f:
                tree_dict = json.load(f)
                b_plus_tree = cls(t=t)
                b_plus_tree.root = dict_to_node(tree_dict, t)
                return b_plus_tree
        except:
            return None


# Demo
if __name__ == "__main__":
    print("=== B+Tree Demo ===\n")
    
    # Create B+Tree with degree t=2 (max 3 keys per node)
    btree = BPlusTree(t=2)
    
    # Insert (row_id, page_id) mappings
    mappings = [
        (1, 1), (2, 1), (3, 2), (4, 2),
        (5, 3), (6, 3), (7, 4), (8, 4),
        (10, 5), (12, 5), (15, 6), (20, 7)
    ]
    
    print("Inserting mappings:")
    for row_id, page_id in mappings:
        btree.insert_row_mapping(row_id, page_id)
        print(f"  Row {row_id} → Page {page_id}")
    
    print("\n=== Tree Structure ===")
    btree.pretty_print()
    
    print("\n=== Search Tests ===")
    test_rows = [1, 5, 12, 99]
    for row_id in test_rows:
        page_id = btree.get_page_id(row_id)
        if page_id:
            print(f"Row {row_id} is on Page {page_id} ✓")
        else:
            print(f"Row {row_id} not found ✗")
    
    print("\n=== Traverse (in-order) ===")
    print(btree.traverse())
    
    print("\n=== Traverse Leaves (linked list) ===")
    print(btree.traverse_leaves())
    
    # Export to JSON
    btree.dump_to_json("bplustree_demo.json")
    print("\n✓ Tree exported to bplustree_demo.json")