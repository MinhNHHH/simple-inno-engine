# BPlusTree (CLRS-style) implementation in Python (visible to user)
# Features:
# - BPlusTreeNode and BTree classes
# - search, insert, split_child, insert_non_full
# - pretty_print to show tree structure by levels
# - demo usage with inserts and searches


class BPlusTreeNode:
    def __init__(self, t, leaf=False):
        self.t = t  # minimum degree (defines the range for number of keys)
        self.keys : list[int] = []  # list of ids
        self.children : list[BPlusTreeNode] = []  # list of child pointers (BPlusTreeNode instances)
        self.leaf = leaf  # is true when node is leaf. Otherwise false.

    def is_full(self):
        # A node is full when number of keys == 2*t - 1
        return len(self.keys) == 2 * self.t - 1

    def find_key_index(self, k):
        print("keys, [find_key_index]", self.keys, k)
        # return the index of the first key >= k
        idx = 0
        while idx < len(self.keys) and self.keys[idx] < k:
            idx += 1
        return idx

class BPlusTree:
    """
    BTree is a B-Tree implementation for indexing rows in the database.
    It is used to store the page_id of the rows in the database.

    BTree work:
        - When a row is inserted, the page_id is inserted into the BTree.
        - When a row is deleted, the page_id is deleted from the BTree.  // not implemented
        - When a row is updated, the page_id is updated in the BTree.
        - When a row is searched, the page_id is searched in the BTree.
        - When a row is traversed, the page_ids are traversed in the BTree.
        - When a BTree is dumped to JSON, the BTree is dumped to a JSON file.
        - When a BTree is loaded from JSON, the BTree is loaded from a JSON file.
    """
    def __init__(self, t):
        if t < 2:
            raise ValueError("Minimum degree t must be at least 2")
        self.t = t
        self.root = BPlusTreeNode(t, leaf=True)

    def search(self, node: BPlusTreeNode, k: int) -> tuple[BPlusTreeNode, int]:
        """Search key k starting from node. Returns (node, index) or (None, None) if not found."""
        i = node.find_key_index(k)
        # If found in this node
        if i < len(node.keys) and node.keys[i] == k:
            return node, i
        # If this node is a leaf, key is not present
        if node.leaf:
            return None, None
        # Otherwise go to the appropriate child
        return self.search(node.children[i], k)

    def split_child(self, parent: BPlusTreeNode, i: int) -> None:
        # Corrected split_child implementation capturing median key properly
        t = self.t
        full_child = parent.children[i]
        new_child = BPlusTreeNode(t, leaf=full_child.leaf)
        median = t - 1
    
        # capture median key
        promoted_key = full_child.keys[median]

        # new_child: keys after median
        new_child.keys = full_child.keys[median + 1:]
        # if internal, move children as well
        if not full_child.leaf:
            new_child.children = full_child.children[median + 1:]

        # shrink full_child keys and children to left half
        full_child.keys = full_child.keys[:median]
        if not full_child.leaf:
            full_child.children = full_child.children[:median + 1]

        # insert new child and promoted key into parent at position i
        parent.children.insert(i + 1, new_child)
        parent.keys.insert(i, promoted_key)

    def insert(self, k):
        root = self.root
        # if root is full, tree grows in height
        if root.is_full():
            new_root = BPlusTreeNode(self.t, leaf=False)
            new_root.children.append(root)
            self.split_child(new_root, 0)
            self.root = new_root
            self._insert_non_full(new_root, k)
        else:
            self._insert_non_full(root, k)

    def _insert_non_full(self, node, k):
        i = len(node.keys) - 1
        if node.leaf:
            # insert key into the correct position in keys list
            node.keys.append(None)  # dummy to extend list
            while i >= 0 and node.keys[i] > k:
                node.keys[i + 1] = node.keys[i]
                i -= 1
            node.keys[i + 1] = k
        else:
            # find child which will have the new key
            while i >= 0 and node.keys[i] > k:
                i -= 1
            i += 1
            # if the found child is full, split it
            if node.children[i].is_full():
                self.split_child(node, i)
                # after split, decide which of the two children to descend to
                if k > node.keys[i]:
                    i += 1
            self._insert_non_full(node.children[i], k)

    def traverse(self, node=None):
        """Return an in-order list of keys from the tree"""
        if node is None:
            node = self.root
        keys = []
        for i, key in enumerate(node.keys):
            if not node.leaf:
                keys.extend(self.traverse(node.children[i]))
            keys.append(key)
        if not node.leaf:
            keys.extend(self.traverse(node.children[len(node.keys)]))
        return keys

    def pretty_print(self):
        """Print the tree level by level (keys only)"""
        from collections import deque
        q = deque[tuple[BPlusTreeNode, int]]([(self.root, 0)])
        last_level = 0
        lines = []
        while q:
            node, lvl = q.popleft()
            if lvl != last_level:
                lines.append("\n")
                last_level = lvl
            lines.append(f"{node.keys}  ")
            if not node.leaf:
                for c in node.children:
                    q.append((c, lvl + 1))
        print("".join(lines))
    
    def dump_to_json(self, filename):
        """
        Dump the BTree structure as JSON to a file.
        """
        import json

        def node_to_dict(node):
            d = {
                "keys": node.keys,
                "values": node.values,
                "leaf": node.leaf
            }
            if not node.leaf:
                d["children"] = [node_to_dict(child) for child in node.children]
            return d

        tree_dict = node_to_dict(self.root)
        with open(filename, "w") as f:
            json.dump(tree_dict, f, indent=4)
    
    @classmethod
    def load_from_json(cls, filename: str) -> 'BPlusTree':
        """
        Load and restore a BPlusTree structure from a JSON file and return a new BTree instance.
        """
        import json

        def dict_to_node(d, t):
            node = BPlusTreeNode(t, leaf=d["leaf"])
            node.keys = d["keys"]
            if not d["leaf"]:
                node.children = [dict_to_node(child, t) for child in d["children"]]
            return node

        with open(filename, "r") as f:
            tree_dict = json.load(f)
            # Try to infer minimum degree t if possible
            # t is at least ceil(max_keys/2), but safest to require the user to provide it outside
            # or fall back to t=2
            inferred_t = 2
            # Try to guess t from the root, if possible
            n_keys = len(tree_dict["keys"])
            if n_keys > 0:
                # max keys per node = 2t-1; so t = ceil((max_keys+1)/2)
                inferred_t = (n_keys + 1) // 2
                if inferred_t < 2:
                    inferred_t = 2
            b_plus_tree = cls(t=inferred_t)
            b_plus_tree.root = dict_to_node(tree_dict, inferred_t)
            return b_plus_tree

# Demo
if __name__ == "__main__":
    # Minimum degree t = 2 => max keys per node = 3 (2*t - 1)
    t = 2
    b = BPlusTree(t)

    # Insert a sequence of keys and print tree after each insert
    demo_keys = [10, 20, 5, 6, 12, 30, 7, 17]
    print("Inserting keys:", demo_keys)
    for k in demo_keys:
        b.insert(k)
        print(f"\nAfter inserting {k}:")
        b.pretty_print()

    # Traverse (in-order) result
    print("\nIn-order traversal:", b.traverse())

    # Search for some keys
    tests = [6, 15, 17, 100]
    for tk in tests:
        node, idx = b.search(b.root, tk)
        if node:
            print(f"Found key {tk} in node with keys {node.keys} at index {idx}.")
        else:
            print(f"Key {tk} not found in the tree.")

    # b.dump_to_json("btree.json")