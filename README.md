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
space making it easy to index them. Hashes are 448 bit long offering about 111.5 bits of security.

Leaf data is persisted in an external key-value store with reference counting. Garbage collection of
the trie is implemented with a stop and copy approach.

#### Hash format


Let `H(x)== BLAKE2B(x,28)` be a hash function with a 224 bit digest.


  - the hash of a leaf node with value v is taken as `H(0x00 || v)`, forcing the last
    bit to 0, and followed by a 223 0's and a 1.

  - the hash of a bud node is the hash of its child

  - the hash of an internal node with children hashes `H_LEFT`and `H_RIGHT`is obtained by
    first computing `H(0x01 || H_LEFT || H_RIGHT)`, forcing the last bit of
    the hash to 0 and appending 223 0's at the end and a 1.

  - the hash of an extender extending is computed as follow. Let `b0,...,b_(n-1)`
    represent the path taken by the extender with 0 for left and 1 for right.
    Prepend 1 in front of the path, and prepend as many 0s as needed in front
    of the 1 to make 224 bits. Prepend the hash of the extender's child.

The goal is to make the root hash independent of the Patricia implementation. The approach
is inspired from the one described by Vitalik Buterin [here](https://ethresear.ch/t/optimizing-sparse-merkle-trees/3751/14).

#### Node storage format

All nodes are stored in an array with 256 bit cells. The constant size makes it easy for nodes
to refer to each other using an index in the array.

This leads to a bit of awkwardness (223 bit hashes, array size limited to 2^32 - 34)
but the potential benefit is that two nodes can fit in a cache line in 64 bit architectures.
If it turns out this doesn't make a noticeable difference, it's straightforward to increase
the size of cells to 320 bits, or introduce variable size cells, and remove much of the
akwardness.

- Internal node:

 - First 223 bits: first 223 bit of the hash of the internal node. The last bit does not
 need to be stored as it's always 0, by convention, and the next 224 bits are just 223 0s
 followed by a 1.

 - 224th bit: 0 if the index refers to the left child, 1 if it refers to the right child.
   Accomodating this bit is the reason the last bit of the hash is set to 0.

 - Next 32 bits: index of either the left or right child. The other child is always stored
   at the index preceding the internal node.

 For reasons which will become clear the index can only grow to 2^32 - 34.

- Extender node:

 - First 223 bits: segment
 - 224th bit: 0
 - Last 32 bits: 2^32 - 33.

- Leaf node:

 - First 224 bits: hash of the value

 - Next 32 bits: from 2^32 - 32  to 2^32 - 1 inclusive. If 2^32 - 32, the value is
   looked up in an external hash-table. Otherwise, if 2^32 - 32 + l, with l > 0,
   then l is the length in byte of the value which is read directly in the
   previous cell. This is helpful to avoid having to hit the  key-value table for
   small values, but it does create some duplication. To be benchmarked...

- Bud node:

 - First 224 bits: 1's
 - Last 32 bits: index of the child node.

  It could seem at first that we might not need to store bud nodes at all, but they are
 helpful when creating snapshots, to preserve the property that a non-indexed
 internal node never points to two indexed children.
