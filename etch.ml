(** Implementation of space-efficient binary Patricia trees in OCaml.
    The implementation is geared for used in Tezos, though it is rather
    generic. A stop-and-copy GC is provided. This implementation aims
    to maximize correctness and cares second about efficiency. Extracting
    and efficient C program from F* should be explored. *)

module Path : sig
  (** A module encapsulating the concept of a path through the Patricia tree.
      A path is a sequence of n full segments. The n-1 first segments end in
      a bud and the nth ends in a leaf. Internal segments are bit of paths
      encoded in the internal nodes of the Patricia tree while tip segments
      represent the bits of path encoded close to the leaf. *)

  type side = Left | Right
  (** Binary tree, by convention when a bool or a bit is used, 0 = false = Left
      and 1 = true = Right *)

  type internal
  (** Phantom type used to tag internal pieces of segments. *)

  type tip
  (** Phantom type used to tap tip pieces of segments. *)

  type full
  (** A full segment, used as part of a path as a key to insert values. *)

  type 'a segment = private side list
  (** A segment is always just a list of sides. TODO: use bit arithmetic for
      speed and memory footprint.*)

  type path = full segment list
  (** A path is a list of full segments. *)

end = struct
  type side = Left | Right
  type internal
  type tip
  type full
  type 'a segment = side list (* FIXME: use bit arithmetic *)
  type path = full segment list
end

module Value : sig
  (** Module encapsulating the values inserted in the Patricia tree. *)

  type t = private string
  (** Type of a value. *)

  type hash
  (** Type for the hash of a value. *)

  val hash : t -> hash
  (** Returns the hash of a value. *)

end = struct
  type t = string
  type hash = string
  let hash value = ignore value; failwith "not implemented"
end


module KeyValueStore : sig
  (** Key-value store for leaf data using reference counting
      for deletion. TODO provide persistence to disk. *)

  (** Type of a key-value store. *)
  type t

  (** New, empty table *)
  val make : unit -> t

  (** Inserts a key in the table, or update the reference
      count if the key is already present. *)
  val insert   : t -> Value.t -> Value.hash

  (** Gets a value from the table, returns None if the key
      is not present. *)
  val get_opt  : t -> Value.hash -> Value.t option

  (** Decrements the reference counter of a key in the table.
      Deletes the key if the counter falls to 0. *)
  val decr     : t -> Value.hash -> unit

end = struct
  type t = (Value.hash, (Value.t * int)) Hashtbl.t
  let make () = Hashtbl.create 0

  let insert table value =
    let h = Value.hash value in
    match Hashtbl.find_opt table h with
    | None -> Hashtbl.add table h (value, 1) ; h
    | Some (_, count) -> Hashtbl.replace table h (value, count+1) ; h
  let get_opt table h =
    match Hashtbl.find_opt table h with
    | None -> None
    | Some (v, _) -> Some v
  let decr table h =
    match Hashtbl.find_opt table h with
    | None -> Printf.printf "none\n" ; ()
    | Some (_, 1) -> Printf.printf "some 1\n" ; Hashtbl.remove table h
    | Some (v, count) -> Printf.printf "some %d\n" count ;
      Hashtbl.replace table h (v, count-1)
end



module Tree : sig

end = struct

  type value = string
  type index = int
  type hash = bytes

  type indexed
  type not_indexed
  type hashed
  type not_hashed

  type context = {


  (* /!\ Beware, GADT festivities below. *)

  type ('ia, 'ha) indexed_implies_hashed =
    | Indexed_and_Hashed : (indexed, hashed) indexed_implies_hashed
    | Not_Indexed_Any : (not_indexed, 'ha) indexed_implies_hashed
    (* Type used to prove that all indexed nodes have been hashed. *)

  type ('ha, 'hb, 'hc) hashed_is_transitive =
    | Hashed : hash -> (hashed, hashed, hashed) hashed_is_transitive
    | Not_Hashed : (not_hashed, 'hb, 'hc) hashed_is_transitive
    (* Type used to prove that if a node is hashed then so are its children.
       The type also provides the hash as a witness.*)

  type ('ia, 'ib, 'ic) internal_node_indexing_rule =
    | All_Indexed : index -> (indexed, indexed, indexed) internal_node_indexing_rule
    | Left_Not_Indexed  : (not_indexed, not_indexed, 'ic) internal_node_indexing_rule
    | Right_Not_Indexed : (not_indexed, 'ib, not_indexed) internal_node_indexing_rule
    (* This rule expresses the following invariant : if a node is indexed, then
       its children are necessarily indexed. Less trivially, if a node is not
       indexed then at least one of its children is not yet indexed. The reason
       is that we never construct new nodes that just point to only existing
       nodes. This property guarantees that when we write internal nodes on
       disk, at least one of the child can be written adjacent to its parent. *)

  type ('ia, 'ib) indexing_rule =
    | Indexed : index -> (indexed, indexed) indexing_rule
    | Not_Indexed : (not_indexed, 'ib) indexing_rule
    (* Type used to prove that if a node is indexed, then so are its
       children. Provides the index as a witness. *)

  type ('ia, 'ha) node =
    | Null : (indexed, hashed ) node
    (* Null nodes, used when the tree is small. As the tree becomes bigger,
       the tip segment of path is always large enough to accomodate the
       rest of the segment and Null nodes become vanishingly unlikely. *)

    | Disk : index -> (indexed, hashed) node
    (* Represents a node stored on the disk at a given index, the node hasn't
       been loaded yet. Although it's considered hash for simplicity's sake,
       reading the hash requires a disk access and is expensive. *)

    | View : ('a, 'ha) view -> ('ia, 'ha) node
    (* A view node is the in-memory structure manipulated by programs in order
       to compute edits to the context. New view nodes can be commited to disk
       once the computations are done. *)

  and ('ia, 'ha) view =
      Internal : (Path.internal Path.segment) * ('ib, 'hb) node * ('ic, 'hc) node
                 * ('ia, 'ib, 'ic) internal_node_indexing_rule
                 * ('ha, 'hb, 'hc) hashed_is_transitive
                 * ('ia, 'ha) indexed_implies_hashed
    (* An internal node , left and right children and an internal path segment
       to represent part of the path followed by the key in the tree. *)

      -> ('ia, 'ha) view
    | Bud : (Path.tip Path.segment) * ('ib, 'hb) node
            * ('ia, 'ib) indexing_rule
            * ('ha, 'hb, 'hb) hashed_is_transitive
            * ('ia, 'ha) indexed_implies_hashed
      -> ('ia, 'ha) view
    (* Buds represent the end of a segment and the beginning of a new tree. They
       are used whenever there is a natural hierarchical separation in the key
       or, in general, when one wants to be able to grab sub-trees. For instance
       the big_map storage of a contract in Tezos would start from a bud. *)

    | Leaf: Path.tip Path.segment * Value.t
            * ('ia, 'ia) indexing_rule
            * ('ha, 'ha, 'ha) hashed_is_transitive
            * ('ia, 'ha) indexed_implies_hashed
      -> ('ia, 'ha) view
    (* Leaf of a tree, the end of a path, contains or points to a value.
       The current implementation is a bit hackish and leaves are written
       on *two* cells, not one. This is important to keep in mind when
       committing the tree to disk.
    *)

  type context =
    {
      array : (char, CamlinternalBigarray.int8_unsigned_elt,
               CamlinternalBigarray.c_layout) Bigarray.Array1.t ;
      (* mmaped array where the nodes are written and indexed. *)

      mutable length : Stdint.uint32 ;
      (* Current length of the node table. *)

      leaf_table  : KeyValueStore.t ;
      (* Hash table  mapping leaf hashes to their values. *)

      roots_table : (hash, index) Hashtbl.t
      (* Hash table mapping root hashes to indices in the array. *)
    }


  let make_internal : type ia ha .
    (Path.internal Path.segment) ->
    (not_hashed, not_indexed) node ->
    (ia, ha) node ->
    Path.side -> node =
    fun internal_segment node fresh old fresh_side ->
      match fresh_side with
      | Left  -> View (
          Internal (internal_segment, fresh, old, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any))
      | Right -> View (
          Internal (internal_segment, old, fresh, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any))

  let rec upsert : type ia ha .
    context ->
    (ia, ha) node ->
    Path.path ->
    Value.t ->
    Path.side -> ('ib, 'hb) node =
    fun context node path value side =
      match (path, node) with

      | ([ ], _) -> failwith "Ran out of segments without terminating."
      (* No segment shouldn't happen. *)

      | (_, Disk index) ->
        upsert context path (load_node context index) value side
      (* If we encounter a node still on the disk, load it and retry. *)

      | ([segment], Null) ->
        if Path.fit_in_leaf segment then
          let leaf_segment = Path.to_leaf segment in
          View (Leaf (leaf_segment, value, Not_Indexed, Not_Hashed, Not_Indexed_Any))
          (* If we can fit the remaining segment in a leaf's segment, just create the leaf. *)
        else
          begin
            let (internal_segment, turn, remaining_segment) = Path.extract_internal segment in
            match turn with
            | None -> assert false
            (* If it all fit in an internal segment, then it would have fitted in a leaf. *)
            | Some direction ->
              make_internal
                internal_segment
                (upsert context Null [remaining_segment] value direction)
                Null
                direction
          end

      | (segment :: rest, Null) ->


      | segment::

end
