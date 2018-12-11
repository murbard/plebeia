# Plebeia

Plebia is an opinionated, functional, implementation of a Merkle Patricia trie persisted on disk.
The library allows the manipulation of in-memory functional views of the trie and perform atomic commits of those views to the disk.

## Principle

### The Trie

The general idea of a trie, or radix trees is to insert key value pairs by following a path in a tree determined by the key.
A Patricia trie optimizes this process by letting edges in the tree represent contiguous segment of the path. In Plebeia,
this optimization only happens in the leaves that is: a leaf is inserted based on the shortest prefix that distinguishes it
from other leafs. This simplifies the design and makes it more storage efficient. Since the keys are typically hashes which
are unlikely to share long prefixes, not much is lost in terms of tree depth.

Instead, Plebeia has a concept of a bud. A bud is a leaf which is also root of new tree. Values are inserted in the trie
using a _list_ of keys. The first key goes from the root of the trie to the a bud, the second key to the next bud, and
so on until the last key which ends in a leaf. This makes it possible to essentially create subtries within the trie.

### Storage

The trie is persisted to disk in an append only fashion. Internal nodes take up exactly 256 bit of disk space while
leaves use 512 bit. Both fit in a cache line. Links are established using 32 bit indices and hashes are all 223 bits.

Leaf data is persisted in an external key-value store with reference counting.

Garbage collection of the trie is implemented with a stop and copy approach.
