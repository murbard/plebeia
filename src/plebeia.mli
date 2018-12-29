(** Module manipulating patricia trees and persisting them to disk *)


type context
(** A context represents the storage of a collection of trees sharing
    nodes on disk. *)

type hash
(** Root hash of a tree. *)

type ('iprev, 'hprev, 'ihole, 'hhole) cursor
(** Cursor in a tree to efficiently search and edit sub-trees. *)

type segment
(** A segment represents a path from the root of a tree to a leaf or
    to the root of a sub-tree. *)

type error

type value

type indexed type not_indexed
type hashed type not_hashed

val open_context : filename:string -> context
(** Opens or creates a new context backed up at a given location
    in the filesystem. *)

val gc: src:context -> hash list -> dest:context -> unit
(** Copies from the src context trees rooted in the hash list
    into a new context. Used for garbage collection. *)

val root : context -> hash -> (('iprev_, 'hprev_, 'ihole_, 'hhole_) cursor, error) result
(** Gets the root cursor corresponding to a given root hash in the
    context. *)

val empty : context -> ('iprev_, 'hprev_, indexed, hashed) cursor
(** Creates a cursor to a new, empty tree. *)

val subtree : ('iprev, 'hprev, 'ihole, 'hhole) cursor -> segment -> (('iprev_, 'hprev_, 'ihole_, 'hhole_) cursor, error) result
(** Moves the cursor down a segment, to the root of a sub-tree. Think
    "cd segment/" *)

val parent : ('iprev, 'hprev, 'ihole, 'hhole) cursor -> ('iprev_, 'hprev_, 'ihole_, 'hhole_)  cursor
(** Moves the cursor back to the parent tree. Think "cd .." *)

val get : ('iprev, 'hprev, 'ihole, 'hhole) cursor -> segment -> (value, error) result
(** Gets a value if present in the current tree at the given
    segment. *)

val insert: ('iprev, 'hprev, 'ihole, 'hhole) cursor -> segment -> value -> (('iprev_, 'hprev_, 'ihole_, 'hhole_) cursor, error) result
(** Inserts a value at the given segment in the current tree.
    Returns the new cursor if successful. *)

val update: ('iprev, 'hprev, 'ihole, 'hhole) cursor -> segment -> value -> ('iprev_, 'hprev_, 'ihole_, 'hhole_) cursor
(** Updates a value at the given segment in the current tree.
    Returns the new cursor if successful. *)

val upsert: ('iprev, 'hprev, 'ihole, 'hhole) cursor -> segment -> value -> (('iprev_, 'hprev_, not_indexed, not_hashed) cursor, error) result
(** Upserts. This can still fail if the segment leads to a subtree. *)

val snapshot: ('iprev, 'hprev, 'ihole, 'hhole) cursor -> segment -> segment -> (('iprev_, 'hprev_, 'ihole_, 'hhole_) cursor, error) result
(** Snapshots a subtree at segment and place a soft link to it at
    another segment location. *)

val commit: ('iprev, 'hprev, 'ihole, 'hhole) cursor -> hash
(** Commits the change made in a cursor to disk. Returns the new root
    hash. *)

val hash: ('iprev, 'hprev, 'ihole, 'hhole) cursor -> hash
(** Computes the hash of the cursor without committing. *)
