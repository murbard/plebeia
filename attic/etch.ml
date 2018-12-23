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

  val dummy_side : side

  type internal
  (** Phantom type used to tag internal pieces of segments. *)

  type leaf
  (** Phantom type used to tag pieces of segments stored in leaves. *)

  type bud
  (** Phantom type used to tag pieces of segments stored in buds. *)

  type full
  (** A full segment, used as part of a path as a key to insert values. *)

  type 'a segment = private side list
  (** A segment is always just a list of sides. TODO: use bit arithmetic for
      speed and memory footprint.*)

  type path = full segment list
  (** A path is a list of full segments. *)

  val fit_in_leaf : full segment -> leaf segment option
  (** If the segment fits in a leaf segment convert it. *)

  val fit_in_bud : full segment -> bud segment option
  (** If the segment fits in a bud segment convert it. *)

  val extract_internal : full segment -> internal segment * side option * full segment
  (** Cuts a segment into an internal segment, a direction, and a remainder *)

  val common_prefix : 'a segment -> 'b segment ->
    internal segment * side option * 'a segment * side option * 'b segment

end = struct
  type side = Left | Right
  let dummy_side = Left
  type internal
  type leaf
  type bud
  type full
  type 'a segment = side list (* FIXME: use bit arithmetic *)
  type path = full segment list

  let leaf_segment_length = 192 - 1
  let internal_segment_length = 16 - 1
  let bud_segment_length = 208 - 1

  let _ =
    assert (bud_segment_length  >= internal_segment_length);
    assert (leaf_segment_length >= internal_segment_length)

  let fit_in_leaf segment =
    if List.length segment <= leaf_segment_length then Some segment else None

  let fit_in_bud segment =
    if List.length segment <= bud_segment_length then Some segment else None


  let extract_internal segment =
    let rec aux (internal, remainder) count =
      if count = 0 then
        (internal, remainder)
      else match remainder with
        | [] -> (internal, [])
        | head :: tail ->
          let (internal', remainder') = aux (internal, tail) (count - 1) in
          (head :: internal', remainder')
    in let (internal, remainder) = aux ([], segment) internal_segment_length in
    match remainder with
    | [] -> (internal, None, [])
    | turn :: rest -> (internal, Some turn, rest)

  let list_with_head = function
    | h :: t -> (Some h, t)
    | [] -> (None, [])

  let common_prefix l1 l2 =
    let rec aux l1 l2 len =
      if len = 0 then
        ([], l1, l2)
      else
        match (l1, l2) with
        | ([], []) -> ([], [], [])
        | (l1, []) -> ([], l1, [])
        | ([], l2) -> ([], [], l2)
        | (h1::t1, h2::t2) ->
          if h1 = h2 then
            let (lcp, r1, r2) = aux t1 t2 (len - 1) in
            (h1 :: lcp, r1, r2)
          else
            ([], l1, l2)
    in let (cp, r1, r2) = aux l1 l2 internal_segment_length in
    let (a1, t1) = list_with_head r1 and (a2, t2) = list_with_head r2 in
    (cp, a1, t1, a2, t2)

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
  (** Module manipulating patricia trees and persisting them to disk *)

  type indexed type not_indexed
  type hashed  type not_hashed

  type ('i, 'h) node
  (** Type of a node in a patricia tree, could be internal, a leaf or a bud. *)

  type context
  (** A context represents the storage of a collection of trees sharing nodes on disk *)

  val upsert : context -> ('i, 'h) node -> Path.path -> Value.t -> Path.side ->
    (not_indexed, not_hashed) node

end = struct

  type index = Stdint.uint32
  type hash = bytes

  type indexed type not_indexed
  type hashed type not_hashed

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
       the leaf and bud segment of path is always large enough to accomodate the
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
    | Internal : ('ib, 'hb) node * ('ic, 'hc) node
                 * ('ia, 'ib, 'ic) internal_node_indexing_rule
                 * ('ha, 'hb, 'hc) hashed_is_transitive
                 * ('ia, 'ha) indexed_implies_hashed
    (* An internal node , left and right children and an internal path segment
       to represent part of the path followed by the key in the tree. *)

    | Extender : Path.

      -> ('ia, 'ha) view
    | Bud : ('ib, 'hb) node
            * ('ia, 'ib) indexing_rule
            * ('ha, 'hb, 'hb) hashed_is_transitive
            * ('ia, 'ha) indexed_implies_hashed
      -> ('ia, 'ha) view
    (* Buds represent the end of a segment and the beginning of a new tree. They
       are used whenever there is a natural hierarchical separation in the key
       or, in general, when one wants to be able to grab sub-trees. For instance
       the big_map storage of a contract in Tezos would start from a bud. *)

    | Leaf: Path.leaf Path.segment * Value.t
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


  let make_internal :
    (Path.internal Path.segment) ->
    (not_indexed, not_hashed) node ->
    ('ib, 'hb) node ->
    Path.side -> (not_indexed, not_hashed) node =
    fun internal_segment fresh other fresh_side ->
      match fresh_side with
      | Left  -> View (
          Internal (internal_segment, fresh, other, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any))
      | Right -> View (
          Internal (internal_segment, other, fresh, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any))

  let load_node context index = ignore (context, index) ;failwith "not implemented"

  let rec upsert : type ia ha .
    context ->
    (ia, ha) node ->
    Path.path ->
    Value.t ->
    Path.side -> ('ib, 'hb) node =
    fun context node path value side ->
      match (path, node) with

      | ([ ], _) -> failwith "Ran out of segments without terminating."
      (* No segment shouldn't happen. *)

      | (_, Disk index) ->
        upsert context (load_node context index) path value side
      (* If we encounter a node still on the disk, load it and retry. *)

      | ([segment], Null) -> begin
        match Path.fit_in_leaf segment with

          | Some leaf_segment ->
            View (Leaf (leaf_segment, value, Not_Indexed, Not_Hashed, Not_Indexed_Any))
          (* If we can fit the remaining segment in a leaf's segment, just create the leaf. *)

          | None ->
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
      end

      | (segment :: remaining_path, Null) -> begin
          match Path.fit_in_bud segment with

          | Some bud_segment ->
            View (Bud (bud_segment,
                       (upsert context Null remaining_path value Path.dummy_side),
                         Not_Indexed, Not_Hashed, Not_Indexed_Any))
          | None ->
            begin
              let (internal_segment, turn, remaining_segment) = Path.extract_internal segment in
              match turn with
              | None -> assert false
              (* If it all fit in an internal segment, then it would have fitted in a bud. *)

              | Some direction ->
                make_internal
                  internal_segment
                  (upsert context Null (remaining_segment::remaining_path) value direction)
                  Null
                  direction
            end
        end

      | (segment :: remaining_path, View view_node) -> begin
          match view_node with
          | Internal (other_internal_segment, left, right, _, _, _) -> begin
              let (internal_segment, turn, segment, other_turn, other_internal_segment) =
                Path.common_prefix segment other_internal_segment in
              begin
                match (turn, other_turn) with
                | (Some Left, None) ->
                  make_internal
                    internal_segment
                    (upsert context left (segment::remaining_path) right value Left)
                    right
                    Left
                | (Some Right, None) ->
                  make_internal
                    internal_segment
                    (upsert context right (segment::remaining_path) left Right)
                    left
                    Right

                | (None, _) -> assert false (* we can't have nothing left *)

                | (Some direction, Some other_direction) ->
                  let new_internal = reduce (
                      View (Internal (other_internal_segment, left, right, Left_Not_Indexed,
                                     Not_Hashed, Not_Indexed_Any))) in
                  if (direction = other_direction) then
                    make_internal
                      internal_segment
                      (upsert context new_internal (segment::remaining_path) value direction)
                      Null
                      direction
                  else
                    make_internal
                      internal_segment
                      (upsert context Null (segment::remaining_path) value direction)
                      new_internal
                      direction



            end
          | Bud (bud_segment, next_root, _, _, _) -> failwith "not implemented"
          | Leaf (other_leaf_segment, other_value, _, _, _) -> failwith "not implemented"
        end

end
