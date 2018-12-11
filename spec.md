The nodes of the patricia trie are stored in a file in sequence.
Each node represents exactly 256 bit. They can be of 3 types:

- Internal node: [ hash - 223 bit | 1 bit to indicate if preceded by left child or right child | index of other child  - 32 bit]]
- Leaf node    : [ path - 254 bit | 2 bits to indicate if preceded by an internal node representing the root of a new tree, the hash of a value, or just a value] 


We also have a hash-table: content_hash -> (ref_count, content)

And one hash-table: context-hash -> index


2^32 * 32 bytes ~ 2^37 bytes ~ 128 Gig
