type hashed (* a node knows its internal hash *)
type not_hashed (* a node that doesn't *)
type indexed (* a node that has been indexed on disk *)
type not_indexed (* a node that hasn't *)
type hash = string
type index = int
type segment = string
type value = float

let null_hash = "000"

type ('ia, 'ha) indexed_implies_hashed =
    | Indexed_and_Hashed : (indexed, hashed) indexed_implies_hashed
    | Not_Indexed_Any : (not_indexed, 'ha) indexed_implies_hashed

  type ('ha, 'hb, 'hc) hashed_is_transitive =
    | Hashed : hash -> (hashed, hashed, hashed) hashed_is_transitive
    | Not_Hashed : (not_hashed, 'hb, 'hc) hashed_is_transitive

  (* This rule expresses the following invariant : if a node is
     indexed, then its children are necessarily indexed. Less
     trivially, if a node is not indexed then at least one of
     its children is not yet indexed. The reason is that we
     never construct new nodes that just point to only existing
     nodes. This property guarantees that when we write internal
     nodes on disk, at least one of the child can be written
     adjacent to its parent. *)

  type ('ia, 'ib, 'ic) indexing_rules =
     | All_Indexed : index -> (indexed, indexed, indexed) indexing_rules
     | Left_Not_Indexed : (not_indexed, not_indexed, 'ic) indexing_rules
     | Right_Not_Indexed : (not_indexed, 'ib, not_indexed) indexing_rules

  type ('ia, 'ha) node =
    | Null : (indexed, hashed ) node
    | Disk : index -> (indexed, hashed) node
    | View : ('a, 'ha) view -> ('ia, 'ha) node
  and ('ia, 'ha) view =
      Internal : ('ib, 'hb) node * ('ic, 'hc) node
                 * ('ia, 'ib, 'ic) indexing_rules
                 * ('ha, 'hb, 'hc) hashed_is_transitive
                 * ('ia, 'ha) indexed_implies_hashed
                 -> ('ia, 'ha) view
    | Bud : segment * ('ib, 'hb) node
            * ('ia, 'ib, 'ib) indexing_rules
            * ('ha, 'hb, 'hb) hashed_is_transitive
            * ('ia, 'ha) indexed_implies_hashed
      -> ('ia, 'ha) view
    | Leaf: segment * value
            * ('ia, 'ia, 'ia) indexing_rules
            * ('ha, 'ha, 'ha) hashed_is_transitive
            * ('ia, 'ha) indexed_implies_hashed
                 -> ('ia, 'ha) view


let rec get_hash : type i. (i, hashed) node -> hash =
  fun node -> match node with
    | Null -> null_hash
    | Disk _ -> assert false
    | View v -> begin
        match v with
        | Internal  (_, _, _, Hashed h, _) -> h
        | Leaf (_, _, _, Hashed h, _) -> h
        | Bud (_, _, _, Hashed h, _) -> h
      end

let hash_of_index index = string_of_int index

let node_hash h1 h2 = String.concat "||" [h1; h2]

let rec hashtree : type i h. (i, h) node -> (i, hashed) node =
  fun node -> match node with
    | Disk _ -> node
    | Null -> node
    | View v -> begin
        match v with
        (* Already hashed *)
        | Internal (_, _, _, Hashed _, _) -> node
        | Leaf (_, _, _, Hashed _, _) -> node
        | Bud (_, _, _, Hashed _, _) -> node

        (* Hash recursively *)

        | Internal (left, right, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any) ->
          let left = hashtree left and right = hashtree right
          in let hash = node_hash (get_hash left) (get_hash right) in
          View (Internal (left, right, Left_Not_Indexed, Hashed hash, Not_Indexed_Any))

        | Internal (left, right, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any) ->
          let left = hashtree left and right = hashtree right
          in let hash = node_hash (get_hash left) (get_hash right) in
          View (Internal (left, right, Right_Not_Indexed, Hashed hash, Not_Indexed_Any))

        | Bud (segment, root, w, Not_Hashed, Not_Indexed_Any) ->
          let root = hashtree root in let hash = get_hash root in
          View (Bud (segment, root, w, Hashed hash, Not_Indexed_Any))

        | Leaf (segment, value, w, Not_Hashed, Not_Indexed_Any) ->
          View( Leaf (segment, value, w, Hashed "", Not_Indexed_Any))

        | _ -> .


      end
