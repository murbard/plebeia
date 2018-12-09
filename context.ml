
let sha192 b:bytes =
  let ret = Bytes.create 24 in
  let digest = Nocrypto.Hash.SHA256.digest (Cstruct.of_bytes b) in
  Cstruct.blit_to_bytes digest 0 ret 0 24 ; ret


(** Key-value store for leaf data using reference counting
    for deletion. TODO provide persistence to disk. *)
module Data : sig
  (** Type of values being inserted in the key value table. *)
  type value

  (** Type of a hash, used as key to the value. *)
  type hash

  (** Type of a key-value store. *)
  type t

  (** Empty table *)
  val empty: unit -> t

  (** Returns the key for a piece of data. *)
  val hash     : value -> hash

  (** Creates a value from a string. *)
  val value_of_string : string -> value

  (** Constructs a hash from bytes. *)
  val hash_of_bytes : bytes -> hash

  (** Inserts a key in the table, or update the reference
      count if the key is already present. *)
  val insert   : t -> value -> hash

  (** Gets a value from the table, returns None if the key
      is not present. *)
  val get_opt  : t -> hash -> value option

  (** Decrements the reference counter of a key in the table.
      Deletes the key if the counter falls to 0. *)
  val decr     : t -> hash -> unit
end = struct
  type value = bytes
  type hash = bytes
  type t = (hash, (value * int)) Hashtbl.t
  let empty () = Hashtbl.create 0
  let hash = sha192
  let hash_of_bytes b = b
  let value_of_string = Bytes.of_string
  let insert table value =
    let h = hash value in
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

(** A trie, replicated on disk. *)
module Tries : sig

  (** A set of trees with shared nodes, with disk storage. *)
  type tries

  (** The root of a tree. *)
  type root

  (** Hash of a node in the tree. *)
  type hash

  (** Key to add values in the tree. *)
  type path

  (** Creates a path out of a hash. *)
  val make_path : bytes -> path

  (** Create a new trie. *)
  val create : namespace:string -> leaf_table:Data.t -> tries

  (** Create a new, empty root in the trie. *)
  val empty  : tries -> root

  (** Commits a root to disk. *)
  val commit : root -> unit

  (** Gets a root by its hash *)
  val get_opt : tries -> hash -> root option

  (** Update or inserts a value in a root. *)
  val upsert : root -> path -> Data.value -> root

  (** Deletes a value from a root. *)
  val delete : root -> path -> root

  (** Delete a list of roots, given by their hash, in the tries. *)
  val gc     : tries -> hash list -> unit

end = struct

  (* Index to access nodes in the arrays.*)
  type index = Stdint.uint32
  type hash = Bytes.t
  type path = Bytes.t

  type tries =
    {
      namespace : string ;
      array : (char, CamlinternalBigarray.int8_unsigned_elt,
               CamlinternalBigarray.c_layout) Bigarray.Array1.t ;
      leaf_table  : Data.t ;
      roots_table : (path, index) Hashtbl.t
    }

  type node =
    | View of view_node * (index option)
    | Disk of index
    | Null
  and view_node =
      Internal of node * node
    | Leaf of path * Data.value

  type root = tries * node

  let empty tries = (tries, Null)

  type side = Left | Right
  let get_bit path d =
    if (int_of_char (Bytes.get path (d / 8))) land (1 lsl (d mod 8)) = 1 then
      Left
    else
      Right

  let array_slice_to_bytes array a b =
    let bytes = Bytes.create (b - a) in
    for i = a to b - 1 do
      Bytes.set bytes i (Bigarray.Array1.get array i)
    done ; bytes

  let load_node tries index =
    if index = Stdint.Uint32.zero then Null
    else
      let offset = 32 * (Stdint.Uint32.to_int index) in
      let bytes = array_slice_to_bytes tries.array offset (offset + 32) in
      if (Bytes.get bytes 0 |> int_of_char) land 1 == 0 then
        let left  = Stdint.Uint32.of_bytes_big_endian bytes 24
        and right = Stdint.Uint32.of_bytes_big_endian bytes 28 in
        View (Internal (Disk left, Disk right), Some index)
      else
        let path = Bytes.sub bytes 0 24
        and leaf_offset = 32 * Stdint.Uint32.(of_bytes_big_endian bytes 25 |> to_int) in
        let data_hash = array_slice_to_bytes tries.array leaf_offset (leaf_offset + 24) in
        match Data.get_opt tries.leaf_table (Data.hash_of_bytes data_hash) with
        | None -> Printf.ksprintf failwith "Unknown key %s" (Bytes.to_string data_hash)
        | Some value -> View (Leaf (path, value), Some index)

  let null_hash = Bytes.init 24 (fun _ -> '\000')

  let get_hash array index =
    let offset = 32 * (Stdint.Uint32.to_int index) in
    array_slice_to_bytes array offset (offset + 24)

  let mixhash h1 h2 =
    sha192 (Bytes.concat Bytes.empty [h1; h2])


  let upsert (tries, node) path value =
    let rec upsert_aux tries path node depth value =
      match node with
      | Disk index ->
        upsert_aux tries path (load_node tries index) depth value
      | Null ->
        let internal = View (Internal (Null, Null), None) in
        upsert_aux tries path internal depth value
      | View (view_node, _) -> begin
          match view_node with
          | Internal (left, right) ->
            if get_bit path depth = Left then
              let new_left = upsert_aux tries path node (depth+1) value in
              View (Internal (new_left, right), None)
            else
              let new_right = upsert_aux tries path node (depth+1) value in
              View (Internal (left, new_right), None)
          | Leaf (other_path, other_value) ->
            if other_path = path then (* same path, update the key *)
              View (Leaf (path, value), None)
            else (* push down the branch *)
              begin
                let rleft = ref Null and rright = ref Null in
                if get_bit path depth = Left then
                  rleft := upsert_aux tries path !rleft (depth+1) value
                else
                  rright := upsert_aux tries path !rright (depth+1) value ;
                if get_bit other_path depth = Left then
                  rleft := upsert_aux tries other_path !rleft (depth+1) other_value
                else
                  rright := upsert_aux tries other_path !rright (depth+1) other_value ;
                View (Internal (!rleft, !rright), None)
              end
        end
    in (tries, upsert_aux tries path node 0 value)

  let write_internal _ _ _ = (null_hash, Stdint.Uint32.zero)
  let write_leaf _ _ _ = (null_hash, Stdint.Uint32.zero)

  let commit (tries, node) =
    let rec commit_aux tries node depth =
      match node with
      | Null -> (null_hash, Stdint.Uint32.zero)
      | Disk index -> (get_hash tries.array index, index)
      | View (_, Some index) -> (get_hash tries.array index, index)
      | View (view_node, None) -> begin
          match view_node with
          | Internal (left, right) ->
            let (hleft, ileft)  = commit_aux tries left  (depth + 1)
            and (hright, iright) = commit_aux tries right (depth + 1) in
            let h = mixhash hleft hright
            in write_internal ileft iright h
          | Leaf (path, value) -> write_leaf path value depth
        end
    in
    let (hash, index) = commit_aux tries node 0 in
    Hashtbl.add tries.roots_table hash index



  let gc _ _ = ()
  let delete _ _ = failwith "delete not implemented"

  let get_opt _ _ = failwith "get_opt not implemented"

  let create ~namespace ~leaf_table =
    {
      namespace = namespace ;
      array = Bigarray.Array1.create Bigarray.char Bigarray.C_layout 10000 ;
      leaf_table  = leaf_table  ;
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
