(** Module manipulating patricia trees and persisting them to disk *)


type context
(** A context represents the storage of a collection of trees sharing
    nodes on disk. *)

type hash
(** Root hash of a tree. *)

type cursor
(** Cursor in a tree to efficiently search and edit sub-trees. *)

type segment
(** A segment represents a path from the root of a tree to a leaf or
    to the root of a sub-tree. *)

type value

val open_context : filename:string -> context
(** Opens or creates a new context backed up at a given location
    in the filesystem. *)

val gc: src:context -> hash list -> dest:context -> unit
(** Copies from the src context trees rooted in the hash list
    into a new context. Used for garbage collection. *)


val root : context -> hash -> cursor option
(** Gets the root cursor corresponding to a given root hash in the
    context. *)

val empty : context -> cursor
(** Creates a cursor to a new, empty tree. *)

val subtree : cursor -> segment -> cursor option
(** Moves the cursor down a segment, to the root of a sub-tree. Think
    "cd segment/" *)

val parent : cursor -> cursor
(** Moves the cursor back to the parent tree. Think "cd .." *)

val get : cursor -> segment -> value option
(** Gets a value if present in the current tree at the given
    segment. *)

val insert: cursor -> segment -> value -> cursor option
(** Inserts a value at the given segment in the current tree.
    Returns the new cursor if successful. *)

val update: cursor -> segment -> value -> cursor option
(** Updates a value at the given segment in the current tree.
    Returns the new cursor if successful. *)

val upsert: cursor -> segment -> value -> cursor option
(** Upserts. This can still fail if the segment leads to a subtree. *)

val snapshot: cursor -> segment -> segment -> cursor option
(** Snapshots a subtree at segment and place a soft link to it at
    another segment location. *)

val commit: cursor -> hash
(** Commits the change made in a cursor to disk. Returns the new root
    hash. *)

val hash: cursor -> hash
(** Computes the hash of the cursor without committing. *)
