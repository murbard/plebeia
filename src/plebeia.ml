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
  (** Whenever we need to consistently pick side that doesn't really correspond
    to a real side. By convention, it is Left. *)

  type segment = private side list
  (** A segment is always just a list of sides. TODO: use bit arithmetic for
      speed and memory footprint.*)

  val empty : segment
  (** The empty segment. *)

  val cut : segment -> (side * segment) option
  (** Cuts a path into a head and tail if not empty. *)

  val common_prefix : segment -> segment -> segment * segment * segment
  (** Factors a common prefix between two segments. *)

  val of_side_list : side list -> segment
  (** Converts a list of side to a segment. *)

  val to_string : segment -> string
  (** String representation of a segment, e.g. "LLLRLLRL" *)

end = struct
  type side = Left | Right
  let dummy_side = Left
  type segment = side list (* FIXME: use bit arithmetic *)
  let cut = function
    | [] -> None
    | h::t -> Some (h,t)
  let empty = []

  let to_string s = String.concat "" (List.map (function Left -> "L" | Right -> "R") s)
  let rec common_prefix seg1 seg2 = match (seg1, seg2) with
    | (h1 :: t1, h2 :: t2) ->
      if h1 = h2 then
        let (prefix, r1, r2) = common_prefix t1 t2 in
        (h1 :: prefix, r1, r2)
      else
        ([], seg1, seg2)
    | ([], _) -> ([], [], seg2)
    | (_, []) -> ([], seg1, [])

  let of_side_list l = l
end

module Value : sig
  (** Module encapsulating the values inserted in the Patricia tree. *)

  type t = private string
  (** Type of a value. *)

  type hash
  (** Type for the hash of a value. *)

  val hash : t -> hash
  (** Returns the hash of a value. *)

  val of_string : string -> t

  val to_string : t -> string

end = struct
  type t = string
  type hash = string
  let hash value = ignore value; failwith "not implemented"
  let of_string s = s
  let to_string s = s
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

type indexed type not_indexed
type hashed type not_hashed
type extender type not_extender type maybe_extender

(* /!\ Beware, GADT festivities below. *)


type ('ia, 'ha) indexed_implies_hashed =
  | Indexed_and_Hashed : (indexed, hashed) indexed_implies_hashed
  | Not_Indexed_Any : (not_indexed, 'ha) indexed_implies_hashed
  (* Type used to prove that all indexed nodes have been hashed. *)

type ('ha, 'hb, 'hc) hashed_is_transitive =
  | Hashed : Bigstring.t -> (hashed, hashed, hashed) hashed_is_transitive
  | Not_Hashed : (not_hashed, 'hb, 'hc) hashed_is_transitive
  (* Type used to prove that if a node is hashed then so are its children.
     The type also provides the hash as a witness.*)

type ('ia, 'ib, 'ic) internal_node_indexing_rule =
  | All_Indexed : int64 -> (indexed, indexed, indexed) internal_node_indexing_rule
  | Left_Not_Indexed  : (not_indexed, not_indexed, 'ic) internal_node_indexing_rule
  | Right_Not_Indexed : (not_indexed, 'ib, not_indexed) internal_node_indexing_rule
  (* This rule expresses the following invariant : if a node is indexed, then
     its children are necessarily indexed. Less trivially, if a node is not
     indexed then at least one of its children is not yet indexed. The reason
     is that we never construct new nodes that just point to only existing
     nodes. This property guarantees that when we write internal nodes on
     disk, at least one of the child can be written adjacent to its parent. *)

type ('ia, 'ib) indexing_rule =
  | Indexed : int64 -> (indexed, indexed) indexing_rule
  | Not_Indexed : (not_indexed, 'ib) indexing_rule
  (* Type used to prove that if a node is indexed, then so are its
     children. Provides the index as a witness. *)

type ('ea, 'eb) extender_rule =
  | Extender    :  (extender, not_extender) extender_rule
  | Not_Extender : (not_extender, 'eb) extender_rule

type ('ia, 'ha, 'ea) node =
  | Disk : int64 -> (indexed, hashed, 'ea) node
  (* Represents a node stored on the disk at a given index, the node hasn't
     been loaded yet. Although it's considered hash for simplicity's sake,
     reading the hash requires a disk access and is expensive. *)

  | View : ('ia, 'ha, 'ea) view -> ('ia, 'ha, 'ea) node
  (* A view node is the in-memory structure manipulated by programs in order
     to compute edits to the context. New view nodes can be commited to disk
     once the computations are done. *)

and ('ia, 'ha, 'ea) view =
  | Internal : ('ib, 'hb, 'eb) node * ('ic, 'hc, 'ec) node
               * ('ia, 'ib, 'ic) internal_node_indexing_rule
               * ('ha, 'hb, 'hc) hashed_is_transitive
               * ('ia, 'ha) indexed_implies_hashed
    -> ('ia, 'ha, not_extender) view

  (* An internal node , left and right children and an internal path segment
     to represent part of the path followed by the key in the tree. *)

  | Bud : ('ib, 'hb, 'eb) node option
          * ('ia, 'ib) indexing_rule
          * ('ha, 'hb, 'hb) hashed_is_transitive
          * ('ia, 'ha) indexed_implies_hashed
    -> ('ia, 'ha, not_extender) view
  (* Buds represent the end of a segment and the beginning of a new tree. They
     are used whenever there is a natural hierarchical separation in the key
     or, in general, when one wants to be able to grab sub-trees. For instance
     the big_map storage of a contract in Tezos would start from a bud. *)

  | Leaf: Value.t
          * ('ia, 'ia) indexing_rule
          * ('ha, 'ha, 'ha) hashed_is_transitive
          * ('ia, 'ha) indexed_implies_hashed
    -> ('ia, 'ha, not_extender) view
  (* Leaf of a tree, the end of a path, contains or points to a value.
     The current implementation is a bit hackish and leaves are written
     on *two* cells, not one. This is important to keep in mind when
     committing the tree to disk.
  *)

  | Extender : Path.segment
                * ('ib, 'hb, not_extender) node
                * ('ia, 'ib) indexing_rule
                * ('ha, 'hb, 'hb) hashed_is_transitive
                * ('ia, 'ha) indexed_implies_hashed
    -> ('ia, 'ha, extender) view
  (* Extender node, contains a path to the next node. Represents implicitely
     a collection of internal nodes where one child is Null. *)



(* A trail represents the content of the memory stack when recursively exploring a tree.
   Constructing these trails from closure would be easier, but it would make it harder
   to port the code to C. The type parameters of the trail keep track of the type of each
   element on the "stack" using a product type. *)
type modified
type unmodified

(* The "modified" rule lets us tell the trail that if its children have changed, then
   the invariants it expects may be broken. For instance, we may have indexed node in
   our trail that should not longer be indexed because their children have been modified.
   Conversely, knowing that a trail is unmodified lets us avoid reserializing already
   serialized nodes. *)
type ('ia, 'ha, 'ib, 'hb, 'ic, 'hc, 'modified) internal_modified_rule =
  | Modified :  ('ia, 'ha, 'ib, 'hb, 'ic, 'hc, modified) internal_modified_rule
  | Unmodified:
      ('ia, 'ib, 'ic) internal_node_indexing_rule *
      ('ha, 'hb, 'hc) hashed_is_transitive ->
    ('ia, 'ha, 'ib, 'hb, 'ic, 'hc, unmodified) internal_modified_rule

type ('ia, 'ha, 'ib, 'hb, 'modified) modified_rule =
  | Modified : ('ia, 'ha, 'ib, 'hb, modified) modified_rule
  | Unmodified:
      ('ia, 'ib) indexing_rule *
      ('ha, 'hb, 'hb) hashed_is_transitive ->
    ('ia, 'ha, 'ib, 'hb, unmodified) modified_rule

(* TODO this trail might need to deal with extended node in a special way, but I'm not sure *)
type ('itrail, 'htrail, 'etrail, 'modified) trail =
  | Top      : ('itrail, 'htrail, 'etrail, 'modified) trail
  | Left     : (* we took the left branch of an internal node *)
      ('iprev_hole * 'iprev_trail,
       'hprev_hole * 'hprev_trail,
       'eprev_hole * 'eprev_trail, 'mod_pred) trail *
      ('iright, 'hright, 'eright) node *
      ('iprev_hole, 'hprev_hole,
       'ihole, 'hhole, 'iright,
       'hright, 'modified) internal_modified_rule *
      ('iprev_hole, 'hprev_hole) indexed_implies_hashed
    -> ('ihole * ('irprev_hole * 'iprev_trail),
        'hhole * ('hprev_hole * 'hprev_trail),
        'ehole * ('eprev_hole * 'eprev_trail),
        'modified) trail

  | Right     : (* we took the right branch of an internal node *)
      ('ileft, 'hleft, 'eleft) node *
      ('iprev_hole * 'iprev_trail,
       'hprev_hole * 'hprev_trail,
       'eprev_hole * 'eprev_trail, 'mod_pred) trail *
      ('iprev_hole, 'hprev_hole,
       'ileft, 'hleft,
       'ihole, 'hhole, 'modified) internal_modified_rule *
      ('iprev_hole, 'hprev_hole) indexed_implies_hashed
    -> ('ihole * ('irprev_hole * 'iprev_trail),
        'hhole * ('hprev_hole * 'hprev_trail),
        'ehole * ('eprev_hole * 'eprev_trail),
        'modified) trail

  | Budded   :
      ('iprev_hole * 'iprev_trail,
       'hprev_hole * 'hprev_trail,
       'eprev_hole * 'eprev_trail,
       'mod_pred) trail *
      ('iprev_hole, 'hprev_hole,
       'ihole, 'hhole, 'modified) modified_rule *
      ('iprev_hole, 'hprev_hole) indexed_implies_hashed
    -> ('ihole * ('irprev_hole * 'iprev_trail),
        'hhole * ('hprev_hole * 'hprev_trail),
        'ehole * ('eprev_hole * 'eprev_trail),
        'modified) trail

  | Extended :
      ('iprev_hole * 'iprev_trail,
       'hprev_hole * 'hprev_trail,
       extender * 'eprev_trail,
       'mod_pred) trail *
      Path.segment *
      ('iprev_hole, 'hprev_hole,
       'ihole, 'hhole, 'modified) modified_rule *
      ('iprev_hole, 'hprev_hole) indexed_implies_hashed
    -> ('ihole * ('irprev_hole * 'iprev_trail),
        'hhole * ('hprev_hole * 'hprev_trail),
        not_extender * (extender * 'eprev_trail),
        'modified) trail
  (* not the use of the "extender" and "not extender" type to enforce
     that two extenders cannot follow each other *)

type context =
  {
    array : Bigstring.t ;
    (* mmaped array where the nodes are written and indexed. *)

    mutable length : int64 ;
    (* Current length of the node table. *)

    leaf_table  : KeyValueStore.t ;
    (* Hash table  mapping leaf hashes to their values. *)

    roots_table : (Bigstring.t, int64) Hashtbl.t
    (* Hash table mapping root hashes to indices in the array. *)
  }

(* The cursor, also known as a zipper combines the information contained in a trail and
   a subtree to represent an edit point within a tree. This is a functional data structure
   that represents the program point in a function that modifies a tree. *)

type ex_node = Node : ('i, 'h, 'e) node -> ex_node
(* existential type for a node *)

type cursor =
    Cursor : ('ihole * 'iprev, 'hhole * 'hprev, 'ehole * 'eprev, 'modified) trail *
             ('ihole, 'hhole, 'ehole) node * context  -> cursor
  (* Existential type for a cursor, enforces that the hole tags match between the trail and the
       Node *)

let (>>=) y f = match y with
  | Ok x -> Ok (f x)
  | Error e -> Error e

let load_node context index = ignore (context, index) ; failwith "not implemented"

let rec below_bud cursor =
  match cursor with
  | Cursor (trail, node, context) ->
    begin
      match node with
      | Disk i -> below_bud (Cursor (trail, (load_node i context), context))
      | View vnode -> begin
          match vnode with
          | Bud (None, _, _, _) -> Error "Nothing beneath this bud"
          | Bud (Some below, indexing_rule, hashed_is_transitive, indexed_implies_hashed) ->
            Ok (Cursor(
              Budded(trail, Unmodified (indexing_rule, hashed_is_transitive), indexed_implies_hashed),
              below,
              context))
          | _ -> Error "Attempted to navigate below a bud, but got a different kind of node."
        end
    end

let rec subtree cursor segment =
  match below_bud cursor with
  | Error e -> Error e
  | Ok (Cursor (trail, node, context)) ->
    begin
      match node with
      | Disk i -> subtree cursor (load_node i context)
      | View vnode -> begin
          match vnode with
          | Leaf _ -> Error "Reached a leaf."
          | Bud _ ->
            if (segment = Path.empty) then
              Ok (below_bud cursor)
            else
              Error "Reached a bud before finishing"
          | Internal (l, r,
                      internal_node_indexing_rule,
                      hashed_is_transitive,
                      indexed_implies_hashed) -> begin
              match Path.cut segment with
              | None -> Error "Ended on an internal node"
              | Some (Left, segment_rest) ->
                let new_trail =
                    Left (
                      trail, r,
                      Unmodified (
                        internal_node_indexing_rule,
                        hashed_is_transitive),
                      indexed_implies_hashed) in
                subtree (Cursor (new_trail, l, context)) segment_rest
              | Some (Right, segment_rest) ->
                let new_trail = Right (
                    l, trail,
                    Unmodified (internal_node_indexing_rule, hashed_is_transitive),
                    indexed_implies_hashed) in
                subtree (Cursor (new_trail, r, context)) segment_rest
            end
          | Extender (extender, node_below,
                       indexing_rule,
                       hashed_is_transitive,
                       indexed_implies_hashed) ->
            let _, remaining_extender, remaining_segment = Path.common_prefix extender segment in
            if remaining_extender = Path.empty then
              let new_trail =
                Extended(trail, extender,
                         Unmodified (
                           indexing_rule,
                           hashed_is_transitive),
                         indexed_implies_hashed) in
              subtree (Cursor (new_trail, node_below, context)) remaining_segment
            else
              Error "No bud at this location, diverged while going down an extender."
        end
    end

let empty_bud = View (Bud (None, Not_Indexed, Not_Hashed, Not_Indexed_Any))

let empty context = Cursor (Top, empty_bud, context)

let make_context ?pos ?(shared=false) ?(length=(-1)) fn =
  let fd = Unix.openfile fn [O_RDWR] 0o644 in
  let array =
    (* length = -1 means that the size of [fn] determines the size of
       [array]. *)
    let open Bigarray in
    array1_of_genarray @@ Unix.map_file fd ?pos
      char c_layout shared [| length |] in
  { array ;
    length = 0L ;
    leaf_table = KeyValueStore.make () ;
    roots_table = Hashtbl.create 1
  }

let rec string_of_tree root indent =
  let indent_string = String.concat "" (List.init indent (fun _ -> " . ")) in
  let Node node = root in
  match node with
    | (Disk index) -> Printf.sprintf "%sDisk %Ld" indent_string index
    | View (Leaf (value, _, _, _)) ->
      Printf.sprintf "%sLeaf %s\n" indent_string (Value.to_string value)
    | View (Bud  (node , _, _, _)) ->
      let recursive =
        match node with
        | Some node -> (string_of_tree (Node node) (indent + 1))
        | None     ->  "Empty"
      in
      Printf.sprintf "%sBud:\n%s" indent_string recursive
    | View (Internal (left, right, _, _, _)) ->
      Printf.sprintf "%sInternal:\n%s%s" indent_string
        (string_of_tree (Node left) (indent + 1))
        (string_of_tree (Node right) (indent + 1))
    | View (Extender (segment, node, _, _, _)) ->
      Printf.sprintf "%s[%s]- %s" indent_string (Path.to_string segment)
        (string_of_tree (Node node) (indent + 1))

type error = string
type value = Value.t
type segment = Path.segment
type hash = Bigstring.t

let commit _ = failwith "not implemented"
let snapshot _ _ _ = failwith "not implemented"
let update _ _ _ = failwith "not implemented"
let insert _ _ _ = failwith "not implemented"
let get _ _ = failwith "not implemented"
let parent _ = failwith "not implemented"
let subtree _ _ = failwith "not implemented"
let gc ~src:_ _ ~dest:_ = failwith "not implemented"
let hash _ = failwith "not implemented"
let open_context ~filename:_ = failwith "not implemented"
let root _ _ = failwith "not implemented"



type ('i, 'h) ex_extender_node =
  | Is_Extender : ('i, 'h, extender) node -> ('i, 'h) ex_extender_node
  | Not_Extender: ('i, 'h, not_extender) node -> ('i, 'h) ex_extender_node


module Utils : sig
  val extend : Path.segment -> (not_indexed, not_hashed, not_extender) node -> ex_node

end = struct

  let extend : Path.segment -> ('i, 'h, not_extender) node ->  ex_node =
    fun segment node ->
      if segment = Path.empty then
        Node node
      else
        Node (View (Extender (segment, node, Not_Indexed, Not_Hashed, Not_Indexed_Any)))
end

(* todo, implement get / insert / upsert by using
   a function returning a zipper and then separate
   functions to do the edit *)

let tag_extender : type e . ('i, 'h, e) view -> ('i, 'h) ex_extender_node =
  fun node -> match node with
    | Extender _ -> Is_Extender  (View node)
    | Internal _ -> Not_Extender (View node)
    | Leaf _ ->     Not_Extender (View node)
    | Bud  _ ->     Not_Extender (View node)

let upsert : cursor ->
  Path.segment ->
  Value.t ->
  (cursor, string)  result =

  fun cursor segment value ->

    let leaf = View (Leaf (value, Not_Indexed, Not_Hashed, Not_Indexed_Any)) in
    let Cursor (_, _, context) = cursor in

    let rec upsert_aux : ex_node ->
      Path.segment ->
      Path.side ->
      ((not_indexed, not_hashed) ex_extender_node , error) result =

      fun (Node node) segment side ->
        match (segment, node) with
        | (_, Disk index) ->
          upsert_aux (load_node context index)  segment side
        (* If we encounter a node still on the disk, load it and retry. *)

        | (segment, View view_node) -> begin
            match view_node with

            | Internal (left, right, _, _, _) -> begin
                let insert_new_node new_node = function
                  | Path.Left ->
                    Not_Extender (View (Internal (new_node, right, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any)))
                  | Path.Right ->
                    Not_Extender (View (Internal (left, new_node, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any)))
                in match Path.cut segment with
                | None -> Error "A segment ended on an internal node"
                | Some (Left, remaining_segment) ->
                  upsert_aux (Node left) remaining_segment Path.Left >>=
                  fun new_left -> begin
                    match new_left with
                    | Is_Extender new_left -> insert_new_node new_left Left
                    | Not_Extender new_left -> insert_new_node new_left Left
                  end
                | Some (Right, remaining_segment) ->
                  upsert_aux (Node right) remaining_segment Path.Right >>=
                  function
                  | Is_Extender new_right -> insert_new_node new_right Right
                  | Not_Extender new_right -> insert_new_node new_right Right
             end
            | Leaf _ ->
              Ok (Not_Extender leaf) (* Upserting !!! *)

            | Bud _ -> Error "Tried to upsert a value into a bud"

            | Extender (extender_segment, other_node, _, _, _) ->
              let (common_segment, remaining_segment, remaining_extender) =
                Path.common_prefix segment extender_segment in begin

                (* here's what can happen
                       - common_prefix is empty : not a problem just create an internal node
                       and inject remaining_segment and remaining_extender right and left
                       (they must differ on the first bit if common_prefix is empty.

                       - common_prefix is not empty : cut the extender to the common prefix,
                       take the remaining_extender and create a second extender in succession
                       then recursively insert with the remaining_segment at the new cross point.
                *)

                match (Path.cut remaining_segment, Path.cut remaining_extender) with

                | (_, None) -> upsert_aux (Node other_node) Path.empty Path.dummy_side
                (* we traveled the length of the extender *)

                | (None, Some _) -> Error "not sure but this smells fishy"

                | (Some (my_dir, remaining_segment), Some (other_dir, remaining_extender)) ->
                  let my_node =
                    if remaining_segment = Path.empty then
                      Not_Extender leaf
                    else
                      Is_Extender (View (Extender (remaining_segment, leaf, Not_Indexed, Not_Hashed, Not_Indexed_Any)))
                  in
                  let other_node =
                    if remaining_extender = Path.empty then
                      Node other_node
                    else
                      Node (View (Extender (remaining_extender, other_node, Not_Indexed, Not_Hashed, Not_Indexed_Any)))
                  in
                  let Node other_node = other_node in
                  let internal = begin
                    let insert_into_internal n = function
                      | Path.Left ->
                        Internal(n, other_node, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any)
                      | Path.Right ->
                        Internal(other_node, n, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any) in
                    match (my_dir, other_dir) with
                    | (Left, Right) ->
                      begin
                        match my_node with
                        | Not_Extender my_node -> insert_into_internal my_node Left
                        | Is_Extender my_node -> insert_into_internal my_node Left
                      end
                    | (Right, Left) ->
                      begin
                        match my_node with
                        | Not_Extender my_node -> insert_into_internal my_node Right
                        | Is_Extender my_node -> insert_into_internal my_node Right
                      end

                    | _ -> assert false (* if there is no common segment, they should differ? *)
                  end
                  in
                  if common_segment = Path.empty then (Ok (Not_Extender (View (internal))))
                  else
                    let e = Extender (
                            common_segment,
                            View internal,
                            Not_Indexed, Not_Hashed, Not_Indexed_Any) in
                    Ok (Is_Extender (View (e)))
              end
          end
    in
    let Cursor (trail, node, _) = cursor in
    upsert_aux (Node node) segment Path.dummy_side >>=
    fun node ->
    let update_trail node =
      match trail with
      | Top -> Cursor (Top, node, context)
      | Left (prev_trail, right, _, indexed_implies_hashed) ->
        Cursor (Left (prev_trail, right, Modified, indexed_implies_hashed), node, context)
      | Right (left, prev_trail, _, indexed_implies_hashed) ->
        Cursor (Right (left, prev_trail, Modified, indexed_implies_hashed), node, context)
      | Budded (prev_trail, _, indexed_implies_hashed) ->
          Cursor (Budded (prev_trail, Modified, indexed_implies_hashed), node, context)
      | Extended _ -> assert false
    in match node with
    | Is_Extender node -> update_trail node
    | Not_Extender node -> update_trail node



let context =
  let fn = Filename.temp_file "plebeia" "test" in
  make_context ~shared:true ~length:1024 fn

let cursor = empty context

let x = upsert cursor Path.empty (Value.of_string "foo")
