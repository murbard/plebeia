# Plebeia

Opinionated, functional, implementation of a Merkle Patricia trie persisted on disk.
The library allows the manipulation of in-memory functional views of the
trie and atomic commits to disk. Each node takes represents exactly 256 bits on disk.

Leaf content is kept in a separate key-value store with ref counting. Tries can
be garbage collected with a stop and copy approach.
