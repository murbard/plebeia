
let sha224 s =
  let ret = Bytes.create 28 in
  let digest = Nocrypto.Hash.SHA256.digest (Cstruct.of_string s) in
  Cstruct.blit_to_bytes digest 0 ret 0 28 ; Bytes.to_string ret

let node_hash ~left ~right =
  sha224 (String.concat "" ["\001"; left; right])

(** Path of a value inside of a trie. *)
module Path : sig
  type segment
  type short_segment
  type long_segment
  type path = segment list
  type side = Left | Right
  val segment_of_string : string -> segment
  val get_bit: segment -> int -> side
end = struct
  type segment = string
  type short_segment = Stdint.uint32
  type long_segment = Stdint.uint64
  type path = segment list
  type side = Left | Right
  let segment_of_string = sha224
  let get_bit segment d =
    if (int_of_char (Bytes.get segment (d / 8))) land (1 lsl (d mod 8)) = 1 then
      Left
    else
      Right
end

(** Values inserted in the trie. *)
module Value : sig
  type t
  type hash
  val hash : t -> hash
  val hash_of_bytes : bytes -> hash
  val of_string : string -> t
  val string_of_hash : hash -> string
end = struct
  type t = string
  type hash = string
  let of_string s = s
  let string_of_hash h = h
  let hash s = sha224 (String.concat "" ["\000"; s])
  let hash_of_bytes b =
    assert (Bytes.length b = 24) ; b
end

(** Key-value store for leaf data using reference counting
    for deletion. TODO provide persistence to disk. *)
module KeyValueStore : sig
  (** Type of values being inserted in the key value table. *)

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
    | Some (v, count) -> Printf.printf "some %d\n" count ; Hashtbl.replace table h (v, count-1)
end

module Prefix : sig
  type t
end = struct
  type t = Stdint.uint32
end
(** A functional Merkle trie with persistence to disk. *)
module Forest : sig

  type hashed
  type not_hashed
  type indexed
  type not_indexed
  type ('ia, 'ha) node

  (** A set of trees with shared nodes, stored on disk. *)
  type forest

  (** The root of a tree in the forest. *)
  type ('i, 'h) root

  (** Hash of a node in the forest. *)
  type hash

  (** Create a new trie. *)
  val create : KeyValueStore.t -> filename:string -> forest

  (** Create a new, empty root in the trie. *)
  val empty  : forest -> (indexed, hashed) root

  (** Commits a hashed root to disk. *)
  val commit : ('i, hashed) root -> (indexed, hashed) root

  val hash : ('i, 'h) root -> ('i, hashed) root

  (** Gets a root by its hash *)
  val get_opt : forest -> hash -> (indexed, hashed) root option

  (** Update or inserts a value in a root. *)
  val upsert : ('ia, 'ha) root -> Path.path -> Value.t -> (not_indexed, not_hashed) root

  (** Deletes a value from a root. *)
  val delete : ('ia, 'ha) root -> Path.path -> ('ib, 'hb) root

  (** Delete a list of roots, given by their hash, in the forest. *)
  val gc     : forest -> hash list -> unit

end = struct

  type hashed (* a node knows its internal hash *)
  type not_hashed (* a node that doesn't *)
  type indexed (* a node that has been indexed on disk *)
  type not_indexed (* a node that hasn't *)
  type hash = string
  type index = Stdint.uint32

  let gc forest path = ignore (forest, path) ; failwith "not implemented"


  let get_opt _ _ = failwith "get_opt not implemented"

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

  type ('ia, 'ib, 'ic) internal_node_indexing_rule =
    | All_Indexed : index -> (indexed, indexed, indexed) internal_node_indexing_rule
    | Left_Not_Indexed : (not_indexed, not_indexed, 'ic) internal_node_indexing_rule
    | Right_Not_Indexed : (not_indexed, 'ib, not_indexed) internal_node_indexing_rule

  type ('ia, 'ib) indexing_rule =
    | Indexed : index -> (indexed, indexed) indexing_rule
    | Not_Indexed : (not_indexed, 'ib) indexing_rule


  type ('ia, 'ha) node =
    | Null : (indexed, hashed ) node
    | Disk : index -> (indexed, hashed) node
    | View : ('a, 'ha) view -> ('ia, 'ha) node
  and ('ia, 'ha) view =
      Internal : Path.short_segment * ('ib, 'hb) node * ('ic, 'hc) node
                 * ('ia, 'ib, 'ic) internal_node_indexing_rule
                 * ('ha, 'hb, 'hc) hashed_is_transitive
                 * ('ia, 'ha) indexed_implies_hashed
      -> ('ia, 'ha) view
    | Bud : Path.long_segment * ('ib, 'hb) node
            * ('ia, 'ib) indexing_rule
            * ('ha, 'hb, 'hb) hashed_is_transitive
            * ('ia, 'ha) indexed_implies_hashed
      -> ('ia, 'ha) view
    | Leaf: Path.long_segment * Value.t
            * ('ia, 'ia) indexing_rule
            * ('ha, 'ha, 'ha) hashed_is_transitive
            * ('ia, 'ha) indexed_implies_hashed
      -> ('ia, 'ha) view

  type forest =
    {
      array : (char, CamlinternalBigarray.int8_unsigned_elt,
               CamlinternalBigarray.c_layout) Bigarray.Array1.t ;
      mutable length : Stdint.uint32 ;
      leaf_table  : KeyValueStore.t ;
      roots_table : (hash, index) Hashtbl.t
    }

  type ('i, 'h) root = forest * ('i, 'h) node

  let empty forest = (forest, Null)

  let array_cell array index =
    let offset = 32 * Stdint.Uint32.to_int index in
    let bytes = Bytes.create 32 in
    for i = offset to offset + 31 do
      Bytes.set bytes i (Bigarray.Array1.get array i)
    done ; bytes

  (* Loads and parse a node from the disk mapped array. *)
  let load_node : forest -> index -> (indexed, hashed) node =
    fun forest index ->
      if index = Stdint.Uint32.zero then Null
      else
        let cell = array_cell forest.array index in
        let marker_bit = ((Bytes.get cell 27 |> int_of_char) land 1 > 0) in
        let child_index = Stdint.Uint32.of_bytes_little_endian cell 28 in
        let previous_index = Stdint.Uint32.(index - one) in

        (* special marker to distinguish leaves from internal nodes *)
        if child_index = Stdint.Uint32.max_int then  (* this is a leaf or a bud *)
          let segment = Bytes.sub cell 0 28 |> Bytes.to_string |> Path.segment_of_string in
          let previous_cell = array_cell forest.array previous_index in
          let data_hash = Bytes.sub previous_cell 0 28 in
          if marker_bit then (* this is just a leaf *)
            match KeyValueStore.get_opt forest.leaf_table (Value.hash_of_bytes data_hash) with
            | None -> Printf.ksprintf failwith "Unknown key %s" (Bytes.to_string data_hash)
            | Some value -> View ( Leaf (segment, value,  Indexed index, Hashed data_hash,  Indexed_and_Hashed) )
          else (* this is a bud *)
            View ( Bud (segment, Disk previous_index, Indexed index, Hashed data_hash, Indexed_and_Hashed ))
        else (* this is an internal node *)
          let (left, right) =
            if marker_bit then
              (child_index, previous_index)
            else
              (previous_index, child_index) in
          View ( Internal (Disk left, Disk right, All_Indexed index, Hashed (Bytes.sub cell 0 28), Indexed_and_Hashed) )


  let null_hash = Bytes.init 28 (fun _ -> '\000')

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

  let hash (forest, node) =
    let rec hash_aux : type i h. forest -> (i, h) node -> (i, hashed) node =
      fun forest node -> match node with
        | Disk index -> load_node forest index
        | Null -> Null
        | View v -> begin
            match v with
            (* Already hashed *)
            | Internal (_, _, _, Hashed _, _) -> node
            | Leaf (_, _, _, Hashed _, _) -> node
            | Bud (_, _, _, Hashed _, _) -> node

            (* Hash recursively *)
            | Internal (left, right, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any) ->
              let left = hash_aux forest left and right = hash_aux forest right
              in let h = node_hash ~left:(get_hash left) ~right:(get_hash right) in
              View (Internal (left, right, Left_Not_Indexed, Hashed h, Not_Indexed_Any))

            | Internal (left, right, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any) ->
              let left = hash_aux forest left and right = hash_aux forest right
              in let h = node_hash ~left:(get_hash left) ~right:(get_hash right) in
              View (Internal (left, right, Right_Not_Indexed, Hashed h, Not_Indexed_Any))

            | Bud (segment, root, w, Not_Hashed, Not_Indexed_Any) ->
              let root = hash_aux forest root in let h = get_hash root in
              View (Bud (segment, root, w, Hashed h, Not_Indexed_Any))

            | Leaf (segment, value, w, Not_Hashed, Not_Indexed_Any) ->
              View( Leaf (segment, value, w, Hashed Value.(string_of_hash (hash value)), Not_Indexed_Any))

            | _ -> .
          end
    in (forest, hash_aux forest node)

  let write_internal forest ileft iright h =
    let b = Bytes.create 32 in
    Bytes.blit b 0 h 0 24;
    Stdint.Uint32.to_bytes_little_endian ileft b 24;
    Stdint.Uint32.to_bytes_little_endian iright b 24;
    let index = forest.length in
    let offset = 32 * (Stdint.Uint32.to_int index) in
    for i = 0 to 31 do
      Bigarray.Array1.set forest.array (offset+i) (Bytes.get b i)
    done ;
    forest.length <- Stdint.Uint32.(index+ one);
    index

  let write_leaf forest path value =
    let h = KeyValueStore.insert forest.leaf_table value in
    let index = forest.length in
    ignore h ; ignore index ; ignore path ;
    failwith "not implemented"

  let commit (forest, node) =
    let rec commit_aux : type i h. forest -> (i, hashed) node -> (indexed, hashed) node  =
      fun forest node ->
        match node with
        | Null -> node
        | Disk _ -> node
        | View v -> begin
            match v with
            (* deal with the already indexed ones *)
            | Internal (_, _, All_Indexed _, _, _) -> View v
            | Leaf (_, _, Indexed _, _, _) -> View v
            | Bud (_, _, Indexed _, _, _) -> View v

            (* some non indexed *)
            | Internal (left, right, Left_Not_Indexed, Hashed h, _) ->
              let right = commit_aux forest right
              and left = commit_aux forest left in
              ignore (right, left) ; (* now commit the node *)
              View (Internal (left, right, All_Indexed (Stdint.Uint32.of_int 42), Hashed h, Indexed_and_Hashed))

            | Internal (left, right, Right_Not_Indexed, Hashed h, _) ->
              let left = commit_aux forest left
              and right = commit_aux forest right in
              ignore (right, left) ; (* now commit the node *)
              View (Internal (left, right, All_Indexed (Stdint.Uint32.of_int 42), Hashed h, Indexed_and_Hashed))

            | Leaf (path, value, _, Hashed h, _ ) -> View (Leaf (path, value, Indexed (Stdint.Uint32.of_int 42), Hashed h, Indexed_and_Hashed))

            | Bud (path, root, _, Hashed h, _ ) ->
              let root = commit_aux forest root in
              ignore (root) ;
              View (Bud (path, root, Indexed (Stdint.Uint32.of_int 42), Hashed h, Indexed_and_Hashed))

            | _ -> .
          end
    in (forest, commit_aux forest node)

    let delete (forest, node) path =
      let rec delete_aux :  type ia ha . forest -> Path.path -> (ia, ha) node -> int ->  ('ib, 'hb) node =
      fun forest path node depth  -> begin
          match (path, node) with

          (* We reached the end of our segments and didn't find the key *)
          (* TODO, report an error? *)
          | ([ ], _) -> node

          (* Load from disk and re-try *)
          | (_, Disk index) ->
            delete_aux forest path (load_node forest index) depth

          (* We reached a null leaf, TODO report an error? *)
          | (_ , Null ) -> node

          (* View node, we can actually match on the structure *)
          | (segment :: rest, View view_node) -> begin
              match view_node with

              (* An internal node, insert in the correct branch *)
              | Internal (left, right, _, _, _) -> begin
                  (* TODO, handle the case where one side is now Null and we can thus raise the node *)
                  if Path.get_bit segment depth = Left then
                    let new_left = delete_aux forest path node (depth+1) in
                    View ( Internal (new_left, right, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any) )
                  else
                    let new_right = delete_aux forest path node (depth+1) in
                    View ( Internal (left, new_right, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any) )
                end

              (* A bud. If paths match, continue inserting deeper, otherwise kick down. *)
              | Bud (bud_segment, next_root, _, _, _) ->
                if segment = bud_segment then (* easy case, we move on to the next segment *)
                  upsert_aux forest rest next_root 0 value
                else (* push bud down *)
                  let new_bud = View ( Bud (bud_segment, next_root, Not_Indexed, Not_Hashed, Not_Indexed_Any) ) in
                  let internal =
                    if Path.get_bit bud_segment depth = Path.Left then
                      View (Internal (new_bud, Null, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any))
                    else
                      View (Internal (Null, new_bud, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any))
                  in
                  upsert_aux forest path internal depth value

              (* A leaf *)
              | Leaf (other_segment, other_value, _, _ , _) ->

                (* same path, update the key *)
                if other_segment = segment then
                  if rest = [] then
                    View (Leaf (segment, value, Not_Indexed, Not_Hashed, Not_Indexed_Any))
                  else begin
                    Printf.printf "WARNING ERASING A LEAF WITH A BUD" ;
                    let bud = View ( Bud (segment, Null, Not_Indexed, Not_Hashed, Not_Indexed_Any) )
                    in upsert_aux forest rest bud 0 value
                  end

                else (* push down the branch *)
                  begin
                    let new_leaf = View (Leaf (other_segment, other_value, Not_Indexed, Not_Hashed, Not_Indexed_Any )) in
                    let internal =
                      if Path.get_bit other_segment depth = Path.Left then
                        View ( Internal (new_leaf, Null, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any)  )
                      else
                        View ( Internal (Null, new_leaf, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any) )
                    in upsert_aux forest path internal depth value
                  end
            end
        end
    in (forest, upsert_aux forest path node 0 value)


  let upsert (forest, node) path value =
    let rec upsert_aux :  type ia ha . forest -> Path.path -> (ia, ha) node -> int -> Value.t -> ('ib, 'hb) node =
      fun forest path node depth value -> begin
          match (path, node) with

          | ([ ], _) -> failwith "ran out of segments"

          (* Load from disk and re-try *)
          | (_, Disk index) ->
            upsert_aux forest path (load_node forest index) depth value

          (* Null node, insert a leaf directly, or a bud if need be *)
          | ([ segment ] , Null ) ->
            if (depth >= 128) (* we can fit the segment in a leaf *)

            View (Leaf (segment, value, Not_Indexed, Not_Hashed, Not_Indexed_Any))

          | (segment :: rest, Null ) ->
            let bud = View ( Bud (segment, Null, Not_Indexed, Not_Hashed, Not_Indexed_Any) ) in
            upsert_aux forest rest bud 0 value

          (* View node, we can actually match on the structure *)
          | (segment :: rest, View view_node) -> begin
              match view_node with

              (* An internal node, insert in the correct branch *)
              | Internal (left, right, _, _, _) -> begin
                  if Path.get_bit segment depth = Left then
                    let new_left = upsert_aux forest path node (depth+1) value in
                    View ( Internal (new_left, right, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any) )
                  else
                    let new_right = upsert_aux forest path node (depth+1) value in
                    View ( Internal (left, new_right, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any) )
                end

              (* A bud. If paths match, continue inserting deeper, otherwise kick down. *)
              | Bud (bud_segment, next_root, _, _, _) ->
                if segment = bud_segment then (* easy case, we move on to the next segment *)
                  upsert_aux forest rest next_root 0 value
                else (* push bud down *)
                  let new_bud = View ( Bud (bud_segment, next_root, Not_Indexed, Not_Hashed, Not_Indexed_Any) ) in
                  let internal =
                    if Path.get_bit bud_segment depth = Path.Left then
                      View (Internal (new_bud, Null, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any))
                    else
                      View (Internal (Null, new_bud, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any))
                  in
                  upsert_aux forest path internal depth value

              (* A leaf *)
              | Leaf (other_segment, other_value, _, _ , _) ->

                (* same path, update the key *)
                if other_segment = segment then
                  if rest = [] then
                    View (Leaf (segment, value, Not_Indexed, Not_Hashed, Not_Indexed_Any))
                  else begin
                    Printf.printf "WARNING ERASING A LEAF WITH A BUD" ;
                    let bud = View ( Bud (segment, Null, Not_Indexed, Not_Hashed, Not_Indexed_Any) )
                    in upsert_aux forest rest bud 0 value
                  end

                else (* push down the branch *)
                  begin
                    let new_leaf = View (Leaf (other_segment, other_value, Not_Indexed, Not_Hashed, Not_Indexed_Any )) in
                    let internal =
                      if Path.get_bit other_segment depth = Path.Left then
                        View ( Internal (new_leaf, Null, Left_Not_Indexed, Not_Hashed, Not_Indexed_Any)  )
                      else
                        View ( Internal (Null, new_leaf, Right_Not_Indexed, Not_Hashed, Not_Indexed_Any) )
                    in upsert_aux forest path internal depth value
                  end
            end
        end
    in (forest, upsert_aux forest path node 0 value)


  let create kv_store ~filename =
    let fd = Unix.openfile filename [Unix.O_RDWR ; Unix.O_CREAT] 0o660 in
    let array = Bigarray.array1_of_genarray (
        Unix.map_file fd
          (* Todo, pick a bigger number, dynamically unmap and remap to extend as the need arise. *)
          ~pos:0L Bigarray.char Bigarray.c_layout true [|1000 * 32|])
    in
    {

      array  ;
      length = Stdint.Uint32.one; (* index 0 is implicitely the null leaf *)
      leaf_table  = kv_store  ;
      roots_table = Hashtbl.create 0
    }

  let make_path b = b


end

(** A set of tries, identified by distinct paths, whose roots
    are themselves placed in a Merkle tree. *)
module Context : sig

  type context

  (** Register a new trie under a given path. *)
  val register_trie : context -> path:string list -> context

end = struct

  type context = unit
  let register_trie _ ~path:_= failwith "register_trie not implemented"

end
