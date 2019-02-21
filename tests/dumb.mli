type segment = Plebeia.Plebeia_impl.Path.side list
type value = Plebeia.Plebeia_impl.value

type context = unit
type error = string

type t
type cursor

val get_root_node : cursor -> t

val empty : context -> cursor
(** Creates a cursor to a new, empty tree. *)

val subtree : cursor -> segment -> (cursor, error) result
(** Moves the cursor down a segment, to the root of a sub-tree. Think
    "cd segment/" *)

val create_subtree: cursor -> segment -> (cursor, error) result
(** Create a subtree (bud). Think "mkdir segment" *)

val parent : cursor -> (cursor, error) result
(** Moves the cursor back to the parent tree. Think "cd .." *)

val get : cursor -> segment -> (value, error) result
(** Gets a value if present in the current tree at the given
    segment. *)

val insert: cursor -> segment -> value -> (cursor, error) result
(** Inserts a value at the given segment in the current tree.
    Returns the new cursor if successful. *)

val upsert: cursor -> segment -> value -> (cursor, error) result
(** Upserts. This can still fail if the segment leads to a subtree. *)

val delete: cursor -> segment -> (cursor, error) result
(** Delete a leaf or subtree. *)

val of_plebeia_node : Plebeia.Plebeia_impl.context -> ('a, 'b, 'c) Plebeia.Plebeia_impl.node -> t

val dot_of_node : t -> string

val dot_of_cursor : cursor -> string
