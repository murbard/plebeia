# GADT in plebeia.ml

plebeia.ml relies heavily on GADT and existential type to check some properties at compile time.
While this can hinder the readability of the code, it's important to ensure the correctness of
this prototype. Once written, and tested, this program can be ported to a version without type
checking.

Overall, we use the type system to discuss the following aspects of nodes inside the Patricia tree:

- Indexing : is a node indexed, i.e. has it been written on disk. This is generally indicated with
  the type `'i`, or `'ia`, `'ib`, `'ic` etc.
- Hashed : has the hash of a node been calculated? This is generally indicated with the type `'h` or
  `'ha`, `'hb`, `'hc` etc
- Extender: is this node an extender (i.e. a node containing a series of sides but no branching).
  These are the heart of the Patricia trie optimization, they represent common segments between
  different branches. Generally noted `'e`, `'ea`, `'eb`, `'ec` etc.

These types tag nodes and let us enforce some properties.

 - Internal indexing rule : if a node is indexed, then its two children must also be index. However,
   if a node is not indexed, then at least one of its children must not be indexed either. This is
   a property we use to avoid storing two indices when serializing an internal node, the non index
   child can always be written in the adjacent cell.

 - Indexing rule : for nodes with a single child, if the node is indexed then its child must be
   indexed too

 - Hash is transitive : if a node is had then all of its children are hashed as well

 - Indexed implies hashed : before a node was commited to disk, its hash must have been calculated
   and stored.

 - There cannot be two extender nodes in a row


These phantom types are also used for the trail type (used to build a zipper). The trail basically
keeps a product of all the types in the trail, for each category. So, for instance, if we visited
two indexed and hashed non extender nodes, and the zipper is expecting an extender node that's neither hashed nor indexed next, the type of the trail would be

`(not_indexed * indexed * indexed, not_hashed * hashed * hashed, extender * not_extender * not_extender)`

In order to make all of this work, we use existential type wrapper. Maybe they rare not needed and there's a fancy GADT notation to avoid them, but I don't know it.

First of the cursor wraps the type of the trail and the "hole" ensuring compatibility.
This is helpful to maintain a sane .mli. We also have a type `ex_node` wrapping a node completely,
and a type `ex_extender_node` which only wraps wether or not a node is an extender.
