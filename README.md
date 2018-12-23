Status: early development, do not expect this to work or even compile at this stage.

# Plebeia

Plebeia is a functional implementation of a sparse Merkle tree through a Patricia trie persisted
on disk. The library allows the manipulation of in-memory functional views of the trie and perform
atomic commits of those views to the disk.

## Sub-trees

Plebeia supports sub-trees. That is, a leaf can either contain a value or the root of a sub-tree. Trees
and sub-trees can be navigated like directories and sub-directories using a cursor implemented with
a zipper.

### Storage

The trie is persisted to a file on disk by appending nodes. All nodes take up excatly 256 bit of disk
space making it easy to index them. Hashes are 448 bit long offering about 223 bits of security.

Leaf data is persisted in an external key-value store with reference counting. Garbage collection of
the trie is implemented with a stop and copy approach.
