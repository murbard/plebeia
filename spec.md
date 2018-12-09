The nodes of the patricia trie are stored in a file in sequence.
Each node represents exactly 256 bit. They can be of 3 types:

- Internal node: [ hash - 192 bit | left child - 32 bit | right child - 32 bit ]
- Leaf node    : [ hash - 192 bit | payload - 32 bit | depth - 8 bit | namespace - 24 bit ]
- Payload node : [ content hash - 256 bit ]

We also have a hash-table: content_hash -> (ref_count, content)

And one hash-table: context-hash -> index


2^32 * 32 bytes ~ 2^37 bytes ~ 128 Gig