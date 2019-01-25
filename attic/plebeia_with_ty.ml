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

  val (@) : segment -> segment -> segment

end = struct
  type side = Left | Right
  let dummy_side = Left
  type segment = side list (* FIXME: use bit arithmetic *)
  let cut = function
    | [] -> None
    | h::t -> Some (h,t)
  let empty = []

  let (@) = (@)
  let to_string s =
    String.concat "" (List.map (function Left -> "L" | Right -> "R") s)
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

let sha224 s =
  let ret = Bytes.create 28 in
  let digest = Nocrypto.Hash.SHA256.digest (Cstruct.of_string s) in
  Cstruct.blit_to_bytes digest 0 ret 0 28 ; Bytes.to_string ret

module Value : sig
  (** Module encapsulating the values inserted in the Patricia tree. *)

  type t = private string
  (** Type of a value. *)

  type hash
  (** Type for the hash of a value. *)

  val hash : t -> hash
  (** Returns the hash of a value. *)

  val big_string_of_hash : hash -> Bigstring.t

  val of_string : string -> t

  val to_string : t -> string

end = struct
  type t = string
  type hash = string
  let hash value = sha224 value
  let of_string s = s
  let to_string s = s
  let big_string_of_hash h = Bigstring.of_string h
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
type extender type not_extender

(* /!\ Beware, GADT festivities below. *)

type ('ia, 'ha) indexed_implies_hashed =
  | Indexed_and_Hashed : (indexed, hashed) indexed_implies_hashed
  | Not_Indexed_Any : (not_indexed, 'ha) indexed_implies_hashed
  (* Type used to prove that all indexed nodes have been hashed. *)

type ('ha, 'hb, 'hc) hashed_is_transitive =
  | Hashed : (hashed, hashed, hashed) hashed_is_transitive
  | Not_Hashed : (not_hashed, 'hb, 'hc) hashed_is_transitive
  (* Type used to prove that if a node is hashed then so are its children.
     The type also provides the hash as a witness.*)

type internal
type not_internal

type ('ia, 'ib, 'ic, 'internal) indexing_rule =
  | Indexed : (indexed, indexed, indexed, 'internal) indexing_rule
  | Left_Not_Indexed  : (not_indexed, not_indexed, 'ic, internal) indexing_rule
  | Right_Not_Indexed : (not_indexed, 'ib, not_indexed, internal) indexing_rule
  | Not_Indexed       : (not_indexed, 'ib, 'ib, not_internal) indexing_rule
  (* This rule expresses the following invariant : if a node is indexed, then
     its children are necessarily indexed. Less trivially, if an internal node is not
     indexed then at least one of its children is not yet indexed. The reason
     is that we never construct new nodes that just point to only existing
     nodes. This property guarantees that when we write internal nodes on
     disk, at least one of the child can be written adjacent to its parent. *)

type 'e extender_witness =
  | Maybe_Extender : 'e extender_witness
  | Not_Extender   : not_extender extender_witness
  | Is_Extender    : extender extender_witness

type 'h hashed_witness =
  | Is_Hashed : Bigstring.t -> hashed hashed_witness
  | Not_Hashed : not_hashed hashed_witness

type 'i indexed_witness =
  | Is_Indexed :  int64 -> indexed indexed_witness
  | Not_Indexed : not_indexed indexed_witness

type ('i, 'h, 'e) node_ty  =
  'i indexed_witness * 'h hashed_witness * 'e extender_witness

type ('ia, 'ha, 'ea) node =
  | Disk : (indexed, hashed, 'ea) node
  (* Represents a node stored on the disk at a given index, the node hasn't
     been loaded yet. Although it's considered hash for simplicity's sake,
     reading the hash requires a disk access and is expensive. *)

  | View : ('ia, 'ha, 'ea) view  -> ('ia, 'ha, 'ea) node
  (* A view node is the in-memory structure manipulated by programs in order
     to compute edits to the context. New view nodes can be commited to disk
     once the computations are done. *)

and ('ia, 'ha, 'ea) view =
  | Internal : ('ib, 'hb, 'eb) descr * ('ic, 'hc, 'ec) descr
               * ('ia, 'ib, 'ic, internal) indexing_rule
               * ('ha, 'hb, 'hc) hashed_is_transitive
               * ('ia, 'ha) indexed_implies_hashed
    -> ('ia, 'ha, not_extender) view

  (* An internal node , left and right children and an internal path segment
     to represent part of the path followed by the key in the tree. *)

  | Bud : ('ib, 'hb, 'eb) descr option
          * ('ia, 'ib, 'ib, not_internal) indexing_rule
          * ('ha, 'hb, 'hb) hashed_is_transitive
          * ('ia, 'ha) indexed_implies_hashed
    -> ('ia, 'ha, not_extender) view
  (* Buds represent the end of a segment and the beginning of a new tree. They
     are used whenever there is a natural hierarchical separation in the key
     or, in general, when one wants to be able to grab sub-trees. For instance
     the big_map storage of a contract in Tezos would start from a bud. *)

  | Leaf: Value.t
          * ('ia, 'ia, 'ia, not_internal) indexing_rule
          * ('ha, 'ha, 'ha) hashed_is_transitive
          * ('ia, 'ha) indexed_implies_hashed
    -> ('ia, 'ha, not_extender) view
  (* Leaf of a tree, the end of a path, contains or points to a value.
     The current implementation is a bit hackish and leaves are written
     on *two* cells, not one. This is important to keep in mind when
     committing the tree to disk.
  *)

  | Extender : Path.segment
                * ('ib, 'hb, not_extender) descr
                * ('ia, 'ib, 'ib, not_internal) indexing_rule
                * ('ha, 'hb, 'hb) hashed_is_transitive
                * ('ia, 'ha) indexed_implies_hashed
    -> ('ia, 'ha, extender) view
  (* Extender node, contains a path to the next node. Represents implicitely
     a collection of internal nodes where one child is Null. *)

and ('i, 'h, 'e) descr = ('i, 'h, 'e) node_ty * ('i, 'h, 'e) node

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

type left type right
type dummy_side = left

type ('ia, 'ha, 'ib, 'hb, 'ic, 'hc, 'modified, 'internal, 'side) modified_rule =
  | Modified_Left: ('ia, 'ha, not_indexed, not_hashed, 'ic, 'hc,  modified, 'internal, left) modified_rule
  | Modified_Right: ('ia, 'ha, 'ib, 'hb, not_indexed, not_hashed, modified, 'internal, right) modified_rule
  | Unmodified:
      ('ia, 'ib, 'ic, 'internal) indexing_rule *
      ('ha, 'hb, 'hc) hashed_is_transitive ->
    ('ia, 'ha, 'ib, 'hb, 'ic, 'hc, unmodified, 'internal, 'side) modified_rule

type ('itrail, 'htrail, 'etrail, 'modified) trail =
  | Top      :  ('ihole * ('iprev_hole * 'itrail),
                'hhole * ('hprev_hole * 'htrail),
                'ehole * ('eprov_hole * 'etrail), 'modified) trail
  | Left     : (* we took the left branch of an internal node *)
      ('iprev_hole * 'iprev_trail,
       'hprev_hole * 'hprev_trail,
       not_extender * 'eprev_trail, 'mod_pred) trail
      * ('iprev_hole, 'hprev_hole, not_extender) node_ty
      * ('iright, 'hright, 'eright) descr
      * ('iprev_hole, 'hprev_hole,
         'ihole, 'hhole, 'iright,
         'hright, 'modified, internal, left) modified_rule
      * ('iprev_hole, 'hprev_hole) indexed_implies_hashed
    -> ('ihole * ('iprev_hole * 'iprev_trail),
        'hhole * ('hprev_hole * 'hprev_trail),
        'ehole * (not_extender * 'eprev_trail), 'modified) trail

  | Right     : (* we took the right branch of an internal node *)
      ('ileft, 'hleft, 'eleft) descr
      * ('iprev_hole * 'iprev_trail,
         'hprev_hole * 'hprev_trail,
         not_extender * 'eprev_trail, 'mod_pred) trail
      * ('iprev_hole, 'hprev_hole, not_extender) node_ty
      * ('iprev_hole, 'hprev_hole, 'ileft, 'hleft, 'ihole, 'hhole,
          'modified, internal, right) modified_rule
      *  ('iprev_hole, 'hprev_hole) indexed_implies_hashed
    -> ('ihole * ('iprev_hole * 'iprev_trail),
        'hhole * ('hprev_hole * 'hprev_trail),
        'ehole * (not_extender * 'eprev_trail), 'modified) trail

  | Budded   :
      ('iprev_hole * 'iprev_trail,
       'hprev_hole * 'hprev_trail,
       not_extender * 'eprev_trail, 'mod_pred) trail
      * ('iprev_hole, 'hprev_hole, not_extender) node_ty
      * ('iprev_hole, 'hprev_hole,
         'ihole, 'hhole, 'ihole, 'hhole, 'modified, not_internal, dummy_side) modified_rule
      * ('iprev_hole, 'hprev_hole) indexed_implies_hashed
    -> ('ihole * ('iprev_hole * 'iprev_trail),
        'hhole * ('hprev_hole * 'hprev_trail),
        'ehole * (not_extender * 'eprev_trail), 'modified) trail

  | Extended :
      ('iprev_hole * 'iprev_trail,
       'hprev_hole * 'hprev_trail,
       extender * 'eprev_trail, 'mod_pred) trail
      * ('iprev_hole, 'hprev_hole, extender) node_ty
      * Path.segment
      * ('iprev_hole, 'hprev_hole,
         'ihole, 'hhole, 'ihole, 'hhole, 'modified, not_internal, dummy_side) modified_rule
      * ('iprev_hole, 'hprev_hole) indexed_implies_hashed
    -> ('ihole * ('iprev_hole * 'iprev_trail),
        'hhole * ('hprev_hole * 'hprev_trail),
        not_extender * (extender * 'eprev_trail), 'modified) trail
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


type ex_node = Ex_Node: ('i, 'h, 'e) descr -> ex_node
(* existential type for a node *)

type ('i, 'h) ex_extender_node = Ex_Extender_Node : ('i, 'h, 'e) descr -> ('i, 'h) ex_extender_node

type cursor =
    Cursor :   ('ihole * 'iprev, 'hhole * 'hprev, 'ehole * 'eprev, 'modified) trail
               * ('ihole, 'hhole, 'ehole) descr
               * context  -> cursor
(* The cursor, also known as a zipper combines the information contained in a
   trail and a subtree to represent an edit point within a tree. This is a
   functional data structure that represents the program point in a function
   that modifies a tree. We use an existential type that keeps the .mli sane
   and enforces the most important: that the hole tags match between the trail
   and the Node *)

let (>>=) y f = match y with
  | Ok x -> Ok (f x)
  | Error e -> Error e
(* Error monad operator. *)

let load_node (type e) index context (wit:e extender_witness) : (indexed, hashed, e) view =
  match wit with
  | Maybe_Extender  -> ignore (context, index) ; failwith "not implemented"
  | Not_Extender -> ignore (context, index) ; failwith "not implemented"
  | Is_Extender -> ignore (context, index) ; failwith "not implemented"
(* Read the node from context.array, parse it and create a view node with it. *)

let attach (type itrail htrail ehole etrail) (trail:(itrail, htrail, ehole * etrail, 'm) trail)
    (descr:('i, 'h, ehole) descr) context =
  (* Attaches a node to a trail even if the indexing type and hashing type is incompatible with
     the trail by tagging the modification. Extender types still have to match. *)
  match trail with
  | Top -> Cursor (Top, descr, context)
  | Left (prev_trail, right, ty, _, indexed_implies_hashed) ->
    Cursor (Left (prev_trail, right, ty, Modified_Left, indexed_implies_hashed), descr, context)
  | Right (left, prev_trail, ty,_, indexed_implies_hashed) ->
    Cursor (Right (left, prev_trail, ty, Modified_Right, indexed_implies_hashed), descr, context)
  | Budded (prev_trail, ty, _,  indexed_implies_hashed) ->
    Cursor (Budded (prev_trail, ty, Modified_Left, indexed_implies_hashed), descr, context)
  | Extended (prev_trail, ty, segment, _, indexed_implies_hashed) ->
    Cursor (Extended (prev_trail, ty, segment, Modified_Left, indexed_implies_hashed), descr, context)

let rec go_below_bud (Cursor (trail, node_descr, context)) =
  (* This function expects a cursor positionned on a bud and moves it one step below. *)
  match node_descr with
  | ((Is_Indexed i, _, ewit) as wit, Disk) -> go_below_bud (Cursor (trail, (wit, View (load_node i context ewit)), context))
  | (ty, View vnode) -> begin
        match vnode with
        | Bud (None,  _, _, _) -> Error "Nothing beneath this bud"
        | Bud (Some below, indexing_rule, hashed_is_transitive, indexed_implies_hashed) ->
          Ok (
            Cursor (
              Budded (trail, ty, Unmodified (indexing_rule, hashed_is_transitive), indexed_implies_hashed), below,  context))
        | _ -> Error "Attempted to navigate below a bud, but got a different kind of node."
      end

let rec go_side side (Cursor (trail, descr, context)) =
  (* Move the cursor down left or down right in the tree, assuming we are on an internal node. *)
  match descr with
  | ((Is_Indexed i, _, ewit) as wit, Disk) -> go_side side (Cursor (trail, (wit, View (load_node i context ewit)), context))
  | (ty, View vnode) -> begin
      match vnode with
      | Internal (left, right, indexing_rule, hashed_is_transitive, indexed_implies_hashed) ->
        begin
          match side with
          | Path.Right ->
            Ok (Cursor (Right (left, trail, ty,
                               Unmodified (indexing_rule, hashed_is_transitive),
                               indexed_implies_hashed), right, context))
          | Path.Left ->
            Ok (Cursor (Left (trail, ty, right,
                              Unmodified (indexing_rule, hashed_is_transitive),
                              indexed_implies_hashed), left, context))
        end
      | _ -> Error "Attempted to navigate right or left of a non internal node"
    end

let rec go_down_extender (Cursor (trail, descr, context)) =
  (* Move the cursor down the extender it points to. *)
  match descr with
  | ((Is_Indexed i, _, ewit) as wit,  Disk) -> go_down_extender (Cursor (trail, (wit, View (load_node i context ewit)), context))
  | (ty, View vnode) -> begin
      match vnode with
      | Extender (segment, below, indexing_rule, hashed_is_transitive, indexed_implies_hashed) ->
        Ok (Cursor (Extended (trail, ty, segment,
                              Unmodified (indexing_rule, hashed_is_transitive),
                              indexed_implies_hashed), below, context))
      | _ -> Error "Attempted to go down an extender but did not find an extender"
    end

let go_up (Cursor (trail, descr, context))  =
  match trail with
  | Top -> Error "cannot go above top"
  | Left (prev_trail, ty, right,
          Unmodified (indexing_rule, hashed_is_transitive), indexed_implies_hashed) ->
    let new_node =
      View (Internal (descr, right, indexing_rule, hashed_is_transitive, indexed_implies_hashed)) in
    Ok (Cursor (prev_trail, (ty, new_node), context))

  | Right (left, prev_trail, ty,
           Unmodified (indexing_rule, hashed_is_transitive), indexed_implies_hashed) ->
    let new_node =
      View (Internal (left, descr, indexing_rule, hashed_is_transitive, indexed_implies_hashed))
    in Ok (Cursor (prev_trail, (ty, new_node), context))

  | Budded (prev_trail, ty,
            Unmodified (indexing_rule, hashed_is_transitive), indexed_implies_hashed) ->
    let new_node =
      View (Bud (Some descr, indexing_rule, hashed_is_transitive, indexed_implies_hashed))
    in Ok (Cursor (prev_trail, (ty, new_node), context))

  | Extended (prev_trail, ty, segment,
              Unmodified (indexing_rule, hashed_is_transitive), indexed_implies_hashed) ->
    let new_node =
      View ( Extender (segment, descr, indexing_rule, hashed_is_transitive, indexed_implies_hashed))
    in Ok (Cursor (prev_trail, (ty, new_node), context))

  (* Modified cases. *)

  | Left (prev_trail, _, right, Modified_Left, _) ->
    let internal = View (Internal (descr, right, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any))
    in Ok (attach prev_trail ((Not_Indexed, Not_Hashed, Not_Extender), internal) context)

  | Right (left, prev_trail, _, Modified_Right, _) ->
    let internal = View (Internal (left, descr, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any))
    in Ok (attach prev_trail ((Not_Indexed, Not_Hashed, Not_Extender), internal) context)

  | Budded (prev_trail, _, Modified_Left, _) ->
    let bud = View ( Bud (Some descr, Not_Indexed, Not_Hashed, Not_Indexed_Any))
    in Ok (attach prev_trail ((Not_Indexed, Not_Hashed, Not_Extender), bud) context)

  | Extended (prev_trail, _, segment, Modified_Left, _) ->
    let extender =  View ( Extender (segment, descr, Not_Indexed, Not_Hashed, Not_Indexed_Any))
    in Ok (attach prev_trail ((Not_Indexed, Not_Hashed, Is_Extender), extender) context)


let rec subtree cursor segment =
  (* returns the cursor for the subtree found by following the segment from the given cursor *)
  match go_below_bud cursor with
  | Error e -> Error e
  | Ok (Cursor (trail, descr, context)) ->
    begin
      match descr with
      | ((Is_Indexed i, _, ewit) as wit, Disk) -> subtree (Cursor (trail, (wit, View (load_node i context ewit)), context)) segment
      | (ty, View vnode) -> begin
          match vnode with
          | Leaf _ -> Error "Reached a leaf."
          | Bud _ ->
            if (segment = Path.empty) then
              Ok (go_below_bud cursor)
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
                      trail, ty, r,
                      Unmodified (
                        internal_node_indexing_rule,
                        hashed_is_transitive),
                      indexed_implies_hashed) in
                subtree (Cursor (new_trail, l, context)) segment_rest
              | Some (Right, segment_rest) ->
                let new_trail = Right (
                    l, trail, ty,
                    Unmodified (internal_node_indexing_rule, hashed_is_transitive),
                    indexed_implies_hashed) in
                subtree (Cursor (new_trail, r, context)) segment_rest
            end
          | Extender (extender, node_below,
                       indexing_rule,
                       hashed_is_transitive,
                       indexed_implies_hashed) ->
            let _, remaining_extender, remaining_segment =
              Path.common_prefix extender segment in
            if remaining_extender = Path.empty then
              let new_trail =
                Extended (trail, ty, extender,
                         Unmodified (
                           indexing_rule,
                           hashed_is_transitive),
                         indexed_implies_hashed) in
              subtree (Cursor (new_trail, node_below, context)) remaining_segment
            else
              Error "No bud at this location, diverged while going down an extender."
        end
    end

let empty context =
  (* A bud with nothing underneath, i.e. an empty tree or an empty sub-tree. *)
  let empty_bud = View (Bud (None, Not_Indexed, Not_Hashed, Not_Indexed_Any)) in
  Cursor (Top, ((Not_Indexed, Not_Hashed, Not_Extender), empty_bud), context)

let make_context ?pos ?(shared=false) ?(length=(-1)) fn =
  let fd = Unix.openfile fn [O_RDWR] 0o644 in
  let array =
    (* length = -1 means that the size of [fn] determines the size of
       [array]. This is almost certainly NOT what we want. Rather the array
       should be made x% bigger than the file (say x ~ 25%). When the array
       is close to being full, the file should be closed and reopened with
       a bigger length.
    *) (* FIXME *)
    let open Bigarray in
    array1_of_genarray @@ Unix.map_file fd ?pos
      char c_layout shared [| length |] in
  { array ;
    length = 0L ;
    leaf_table = KeyValueStore.make () ;
    roots_table = Hashtbl.create 1
  }

type error = string
type value = Value.t
type segment = Path.segment
type hash = Bigstring.t

module Utils : sig
  val extend : Path.segment -> (not_indexed, not_hashed, not_extender) descr-> ex_node

end = struct

  let extend segment descr =
      if segment = Path.empty then
        Ex_Node descr
      else begin
        let ty  = (Not_Indexed, Not_Hashed, Is_Extender) in
        Ex_Node (ty, View (Extender (segment, descr, Not_Indexed, Not_Hashed, Not_Indexed_Any)))
      end
end

(* todo, implement get / insert / upsert by using
   a function returning a zipper and then separate
   functions to do the edit *)

let alter cursor segment alteration =
    let leaf value =
      alteration value >>= fun value ->
      View (Leaf (value, Not_Indexed, Not_Hashed, Not_Indexed_Any)) in

    let Cursor (_, _, context) = cursor in

    let rec alter_aux :  ex_node ->
      Path.segment ->
      ((not_indexed, not_hashed) ex_extender_node , error) result =

      fun (Node node) segment side ->
        match (segment, node) with
        | (_, Disk (index, wit)) ->
              alter_aux (Node (View (load_node context index wit)))  segment side
        (* If we encounter a node still on the disk, load it and retry. *)
        | (segment, View view_node) -> begin
            match view_node with
            | Internal (left, right, _, _, _) -> begin
                let insert_new_node new_node = function
                  | Path.Left ->
                    Not_Extender (View (Internal (
                        new_node, right, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any)))
                  | Path.Right ->
                    Not_Extender (View (Internal (
                        left, new_node, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any)))
                in match Path.cut segment with
                | None -> Error "A segment ended on an internal node"
                | Some (Left, remaining_segment) ->
                  alter_aux (Node left) remaining_segment Path.Left >>=
                  fun new_left -> begin
                    match new_left with
                    | Is_Extender new_left -> insert_new_node new_left Left
                    | Not_Extender new_left -> insert_new_node new_left Left
                    (* this pattern feels silly, surely these is a better way *)
                  end
                | Some (Right, remaining_segment) ->
                  alter_aux (Node right) remaining_segment Path.Right >>=
                  function
                  | Is_Extender new_right -> insert_new_node new_right Right
                  | Not_Extender new_right -> insert_new_node new_right Right
                  (* ditto *)
             end
            | Leaf (v, _, _, _) -> leaf (Some v) >>= fun l -> Not_Extender l (* Altering *)
            | Bud _ -> Error "Tried to alter a value into a bud"
            | Extender (extender_segment, other_node, _, _, _) ->
              (* This is the one that is trickier to handle. *)
              let (common_segment, remaining_segment, remaining_extender) =
                Path.common_prefix segment extender_segment in begin
                (* here's what can happen
                   - common_prefix is empty: that means remaining segment
                     and remaining extender start with different directions.
                     So, create an internal node and inject them on each side.

                   - common_prefix is not empty : cut the extender to the
                      common prefix, take the remaining_extender and create a
                      second extender in succession then recursively insert
                      with the remaining_segment at the new cross point.
                *)
                match (Path.cut remaining_segment, Path.cut remaining_extender) with

                | (_, None) -> alter_aux (Node other_node) Path.empty Path.dummy_side
                (* we traveled the length of the extender *)
                | (None, Some _) ->
                  Error "The segment is a prefix to some other path in the tree."
                | (Some (my_dir, remaining_segment),
                   Some (other_dir, remaining_extender)) ->

                  let my_node =
                    if remaining_segment = Path.empty then
                      leaf None >>= fun l -> Not_Extender l
                    else
                      leaf None >>= fun l ->
                      Is_Extender
                        (View (Extender
                                 (remaining_segment, l,
                                  Not_Indexed, Not_Hashed, Not_Indexed_Any)))
                        (* wrap inside an extender existnetial time, this seems
                           silly but I don't know a better way. *)
                  and other_node =
                    if remaining_extender = Path.empty then
                      Node other_node
                    else
                      Node
                        (View (Extender
                                 (remaining_extender, other_node,
                                  Not_Indexed, Not_Hashed, Not_Indexed_Any)))
                        (* This one we complextely wrap in a node existential
                           type. *)
                  in
                  let Node other_node = other_node in
                  let internal = begin
                    let insert_into_internal n = function
                      | Path.Left ->
                        Internal(n, other_node, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any)
                      | Path.Right ->
                        Internal(other_node, n, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any)
                    in match (my_dir, other_dir) with
                    | (Left, Right) ->
                      begin
                        match my_node with
                        | Error e -> Error e
                        | Ok (Not_Extender my_node) -> Ok (insert_into_internal my_node Left)
                        | Ok (Is_Extender my_node)-> Ok (insert_into_internal my_node Left)
                        (* again, very silly *)
                      end
                    | (Right, Left) ->
                      begin
                        match my_node with
                        | Error e -> Error e
                        | Ok (Not_Extender my_node) -> Ok (insert_into_internal my_node Right)
                        | Ok (Is_Extender my_node) -> Ok (insert_into_internal my_node Right)
                      end

                    | _ -> assert false
                  end
                  in
                  match internal with
                  | Error e -> Error e
                  | Ok internal -> begin
                      if common_segment = Path.empty then
                        (Ok (Not_Extender (View internal)))
                      else
                        let e = Extender ( common_segment, View internal,
                                           Not_Indexed, Not_Hashed, Not_Indexed_Any)
                        in Ok (Is_Extender (View (e)))
                    end
              end
          end
    in

    (* When we insert, we expect the cursor to be above a bud, but
       we first have to must past it. However, that's only if there is
       something underneath. *)
    match cursor with
    | Cursor (trail, View (Bud (None, _, _, _)), context) ->
      (* If we're inserting from a bud that is empty below, create the node directly. *)
      if segment = Path.empty then
        Error "Can't insert under a bud with an empty path"
      else
        leaf None >>= fun l ->
        let result =   View (Extender (segment, l, Not_Indexed, Not_Hashed, Not_Indexed_Any)) in
        (attach trail (View (Bud (
            Some result, Not_Indexed, Not_Hashed, Not_Indexed_Any))) context)
    | Cursor (trail, View (Bud (Some insertion_point, _, _, _)), context) ->
      let result = alter_aux (Node insertion_point) segment Path.Left in begin
      match result with
      | Error e -> Error e
      | Ok (Is_Extender result) ->
        Ok (attach trail (View (Bud (Some result, Not_Indexed, Not_Hashed, Not_Indexed_Any))) context)
      | Ok (Not_Extender result) ->
        Ok (attach trail (View (Bud (Some result, Not_Indexed, Not_Hashed, Not_Indexed_Any))) context)
    end
    | _ -> Error "Must insert from a bud"


let delete cursor segment =
  (* this will likely be merged with alter, but for now it's easier to explore that code
     independently. It's also an opportunity to try and get rid of all the existential
     type wrappers *)
  (* strategy : we recursively call delete in the branch, it will either returns a node
     with the deletion done, or it will return nothing, in which case we potentially
     propagate the nothingness.
     Situations where this might be tricky: if one lef of an internal node disappears, then
     it is replaced by the other child. We may thus need to merge extenders but that's OK
  *)
  let Cursor (_, _, context) = cursor in
  let rec delete_aux (Ex_Node node) segment : ((not_indexed, not_hashed) ex_extender_node option, string) result =
    match (node) with
    | ((Is_Indexed i, _, ewit) as wit, Disk) -> delete_aux (Ex_Node (wit, View (load_node i context ewit))) segment
    | (ty, View vnode) -> begin
        match vnode with
        | Leaf _ ->
          if segment = Path.empty then
            Ok None
          else
            Error "reached leaf before end of segment"
        | Bud _ ->  Error "reached a bud and not a leaf"
        | Extender (other_segment, node, _, _, _) ->
          (* If I don't go fully down that segment, that's an error *)
          let _, rest, rest_other = Path.common_prefix segment other_segment in
          if rest_other = Path.empty then begin
            match (delete_aux (Ex_Node node) rest) with
            | Error e -> Error e
            | Ok None -> Ok None
            | Ok (Some (Ex_Extender node)) ->


              Ok (Some (Is_Extender (View (
                Extender(other_segment, node, Not_Indexed, Not_Hashed, Not_Indexed_Any)))))
            | Ok (Some (Is_Extender (View (Extender (next_segment, node, _, _, _))))) ->
              Ok (Some (Is_Extender (View (
                  Extender( Path.(other_segment @ next_segment), node, Not_Indexed, Not_Hashed,
                            Not_Indexed_Any)))))
          end else
            Error (Printf.sprintf "no leaf at this location %s" (Path.to_string rest_other))

        | Internal (left, right, _, _, _) -> begin
            match Path.cut segment with
            | None -> Error "didn't reach a leaf"
            | Some (Left, rest_of_segment) -> begin
                let new_node new_left right =
                  Ok (Some ( Not_Extender (View (Internal (
                      new_left, right, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any)))))
                in
                match delete_aux (Node left) rest_of_segment with
                | Error e -> Error e
                | Ok None -> begin
                    let extend ?(segafter=Path.empty) right =
                      Ok (Some (Is_Extender (View (
                          Extender (Path.((of_side_list [Right]) @ segafter),
                                    right, Not_Indexed, Not_Hashed, Not_Indexed_Any)))))
                    in match right with
                    | Disk (index, wit) -> begin
                        match (load_node index context wit) with
                        | Extender (segment, node, _, _, _) -> extend ~segafter:segment (node)
                        | Internal _ as right -> extend (View right)
                        | Bud _ as right -> extend (View right)
                        | Leaf _ as right -> extend (View right)
                      end
                    | View (Extender (segment, node, _, _, _)) -> extend ~segafter:segment (node)
                    | View (Internal _) -> extend right
                    | View (Bud _)      -> extend right
                    | View (Leaf _)     -> extend right
                  end
                | Ok (Some  (Is_Extender new_left))-> new_node new_left right
                | Ok (Some  (Not_Extender new_left))-> new_node new_left right
              end
            | Some (Right, rest_of_segment) -> begin
                let new_node left new_right =
                  Ok (Some ( Not_Extender (View (Internal (
                      left, new_right, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any)))))
                in match delete_aux (Node right) rest_of_segment with
                | Error e -> Error e
                | Ok None -> begin
                    let extend ?(segafter=Path.empty) left =
                      Ok (Some (Is_Extender (View (
                          Extender (Path.((of_side_list [Left]) @ segafter),
                                    left, Not_Indexed, Not_Hashed, Not_Indexed_Any)))))
                    in match left with
                    | Disk (index, wit) -> begin
                        match (load_node index context wit) with
                        | Extender (segment, node, _, _, _) -> extend ~segafter:segment (node)
                        | Internal _ as right -> extend (View right)
                        | Bud _ as right -> extend (View right)
                        | Leaf _ as right -> extend (View right)
                      end
                    | View (Extender (segment, node, _, _, _)) -> extend ~segafter:segment (node)
                    | View (Internal _) -> extend left
                    | View (Bud _) -> extend left
                    | View (Leaf _) -> extend left
                  end
                | Ok (Some (Is_Extender new_right)) -> new_node left new_right
                | Ok (Some  (Not_Extender new_right))-> new_node left new_right

              end
          end
      end
  in
  match cursor with
  | Cursor (_, View (Bud (None, _, _, _)), _) ->
    (* If we're deleting from a bud that has nothing below... *)
    Error "nothing to delete"
  | Cursor (trail, View (Bud (Some deletion_point, _, _, _)), context) ->
    let result = delete_aux (Node deletion_point) segment in begin
      match result with
      | Error e -> Error e
      | Ok None -> Ok (attach trail (
          View (Bud (None, Not_Indexed, Not_Hashed, Not_Indexed_Any))) context)
      | Ok (Some (Is_Extender result)) ->
        Ok (attach trail (
            View (Bud (Some result, Not_Indexed, Not_Hashed, Not_Indexed_Any))) context)
      | Ok (Some (Not_Extender result)) ->
        Ok (attach trail (
            View (Bud (Some result, Not_Indexed, Not_Hashed, Not_Indexed_Any))) context)
    end
  | _ -> Error "Must insert from a bud"

(* How to merge alter and delete *)
type modifier = value option -> (value option, error) result

(* Replace a part of the tree with another *)
type mogrify = ex_node option -> (ex_node option, error) result


let upsert cursor segment value =
  alter cursor segment (function _ -> (Ok value))

let insert cursor segment value =
  alter cursor segment (
    function
    | None -> (Ok value)
    | Some _ -> Error "leaf already present for this path")


(* What follows is just for debugging purposes, to be removed. *)

let rec string_of_tree root indent =
  (* pretty prints a tree to a string *)
  let indent_string = String.concat "" (List.init indent (fun _ -> " . ")) in
  let Node node = root in
  match node with
    | (Disk (index, _)) -> Printf.sprintf "%sDisk %Ld" indent_string index
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

let context =
  let fn = Filename.temp_file "plebeia" "test" in
  make_context ~shared:true ~length:1024 fn

let (|>>=) x f = match x with
  | Error e -> Error e
  | Ok x -> f x

let cursor = empty context

let foo =
  upsert cursor (Path.of_side_list [Path.Left; Path.Right]) (Value.of_string "fooLR") |>>=
  fun cursor -> (upsert cursor (Path.of_side_list [Path.Left; Path.Left]) (Value.of_string "fooLL"))
  |>>= go_below_bud |>>= go_down_extender |>>= (go_side Path.Left) |>>= go_up
  |>>= (go_side Path.Right) |>>= go_up |>>= go_up |>>= go_up


type ex_hashed_node =
  | Hashed_and_Indexed :    (indexed, hashed, 'e) node * hash  -> ex_hashed_node
  | Hashed_but_not_Indexed : (not_indexed, hashed, 'e) node * hash -> ex_hashed_node

let hash Cursor (trail, node, context) =
  let rec hash_aux (Node node) =  match node with
    (* If it's already hashed nothing to do *)
    | Disk (index, wit) -> hash_aux (Node (View (load_node index context wit)))
    | View vnode -> begin
        match vnode with
        (* easy case where it's already hashed *)
        | Leaf (_, Not_Indexed, Hashed h, _) -> Hashed_but_not_Indexed (node, h)
        | Leaf (_, Indexed _, Hashed h, _) -> Hashed_and_Indexed (node, h)

        | Bud (_, Not_Indexed, Hashed h, _) -> Hashed_but_not_Indexed (node, h)
        | Bud (_, Indexed _, Hashed h, _) -> Hashed_and_Indexed (node, h)

        | Internal (_, _, Left_Not_Indexed, Hashed h, _)  -> Hashed_but_not_Indexed (node, h)
        | Internal (_, _, Right_Not_Indexed, Hashed h, _)  -> Hashed_but_not_Indexed (node, h)
        | Internal (_, _, Indexed _, Hashed h, _)  -> Hashed_and_Indexed (node, h)

        | Extender (_, _, Not_Indexed, Hashed h, _) -> Hashed_but_not_Indexed (node, h)
        | Extender (_, _, Indexed _, Hashed h, _) -> Hashed_and_Indexed (node, h)


        (* hashing is necessary below *)
        | Leaf (value, _, _, _) ->
          let h = Value.(value |> hash |> big_string_of_hash) in
           Hashed_but_not_Indexed ((View (Leaf (value, Not_Indexed, Hashed h, Not_Indexed_Any))), h)
        | Bud (Some underneath, _, _, _) ->
          (match hash_aux (Node underneath) with
           | Hashed_and_Indexed (underneath, h) ->
          (* a bud has the same hash as the node immediately below, its a bit
             of a pseudo node *)
             Hashed_but_not_Indexed (View (Bud (
                 Some underneath, Not_Indexed, Hashed h, Not_Indexed_Any)), h)
           | Hashed_but_not_Indexed (underneath, h) ->
             Hashed_but_not_Indexed (View (Bud (
                 Some underneath, Not_Indexed, Hashed h, Not_Indexed_Any)), h)
          )

        | Internal (left, right, Left_Not_Indexed, _, _) -> (
            match left with
            | View (Leaf _) ->
            | View (Bud _)  -> failwith "foo"
            | View (Internal _) -> failwith "foo"
            | View (Extender _) -> failwith "foo"
            | _ -> .
          )
        | _ -> failwith "moo"
          (*

          let hl = hash_aux (Node left) and hr = hash_aux (Node right) in (
            match (hl, hr) with
            | (Hashed_and_Indexed _, _) -> .
            | (Hashed_but_not_Indexed (l, lh), Hashed_and_Indexed (r, rh)) ->
              let h = Bigstring.concat "||" [lh; rh] in
              Hashed_but_not_Indexed (View (
                  Internal (l, r, Left_Not_Indexed, Hashed h, Not_Indexed_Any)), h)
            | (Hashed_but_not_Indexed (l, lh), Hashed_but_not_Indexed (r, rh)) ->
              let h = Bigstring.concat "||" [lh; rh] in
              Hashed_but_not_Indexed (View (
                  Internal (l, r, Left_Not_Indexed, Hashed h, Not_Indexed_Any)), h)
          )
          *)
      end
      in ignore (hash_aux, ())





let commit _ = failwith "not implemented"
let snapshot _ _ _ = failwith "not implemented"
let update _ _ _ = failwith "not implemented"
let get _ _ = failwith "not implemented"
let delete _ _ = failwith "not implemented"
let create_subtree _ _ = failwith "not implemented"
let parent _ = failwith "not implemented"
let subtree _ _ = failwith "not implemented"
let gc ~src:_ _ ~dest:_ = failwith "not implemented"

let open_context ~filename:_ = failwith "not implemented"
let root _ _ = failwith "not implemented"
