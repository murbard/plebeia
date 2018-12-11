
let sha224 s =
  let ret = Bytes.create 28 in
  let digest = Nocrypto.Hash.SHA256.digest (Cstruct.of_string s) in
  Cstruct.blit_to_bytes digest 0 ret 0 28 ; Bytes.to_string ret

let node_hash ~left ~right =
  sha224 (String.concat "" ["\001"; left; right])

(** Path of a value inside of a trie. *)
module Path : sig
  type segment
  type path = segment list
  type side = Left | Right
  val segment_of_string : string -> segment
  val get_bit: segment -> int -> side
end = struct
  type segment = string
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
end = struct
  type t = string
  type hash = string
  let of_string s = s
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




module Node : sig


end = struct

  type hashed (* a node knows its internal hash *)
  type not_hashed (* a node that doesn't *)
  type indexed (* a node that has been indexed on disk *)
  type not_indexed (* a node that hasn't *)
  type hash = string
  type index = Stdint.uint32

  type ('ia, 'ha) indexed_implies_hashed =
    | Indexed_and_Hashed : (indexed, hashed) indexed_implies_hashed
    | Not_Indexed_Any : (not_indexed, 'ha) indexed_implies_hashed

  type ('ha, 'hb, 'hc) hashed_is_transitive =
    | Hashed : hash -> (hashed, hashed, hashed) hashed_is_transitive
    | Not_Hashed : (not_hashed, 'hb, 'hc) hashed_is_transitive

  type ('ia, 'ib, 'ic) indexed_is_transitive =
    | Indexed : index -> (indexed, indexed, indexed) indexed_is_transitive
    | Not_Indexed : (not_indexed, 'ib, 'ic) indexed_is_transitive

  type ('ia, 'ha) node =
    | Null : (indexed, hashed ) node
    | Disk : index -> (indexed, hashed) node
    | View : ('a, 'ha) view -> ('ia, 'ha) node
  and ('ia, 'ha) view =
      Internal : ('ib, 'hb) node * ('ic, 'hc) node
                 * ('ia, 'ib, 'ic) indexed_is_transitive
                 * ('ha, 'hb, 'hc) hashed_is_transitive
                 * ('ia, 'ha) indexed_implies_hashed
                 -> ('ia, 'ha) view
    | Bud : Path.segment * ('ib, 'hb) node
            * ('ia, 'ib, 'ib) indexed_is_transitive
            * ('ha, 'hb, 'hb) hashed_is_transitive
            * ('ia, 'ha) indexed_implies_hashed
      -> ('ia, 'ha) view
    | Leaf: Path.segment * Value.t
            * ('ia, 'ia, 'ia) indexed_is_transitive
            * ('ha, 'ha, 'ha) hashed_is_transitive
            * ('ia, 'ha) indexed_implies_hashed
      -> ('ia, 'ha) view

end



(** A functional Merkle trie with persistence to disk. *)
module Forest : sig

  (** A set of trees with shared nodes, stored on disk. *)
  type forest

  (** The root of a tree in the forest. *)
  type root

  (** Hash of a node in the forest. *)
  type hash


  (** Create a new trie. *)
  val create : KeyValueStore.t -> filename:string -> forest

  (** Create a new, empty root in the trie. *)
  val empty  : forest -> root

  (** Commits a root to disk. *)
  val commit : root -> unit

  (** Gets a root by its hash *)
  val get_opt : forest -> hash -> root option

  (** Update or inserts a value in a root. *)
  val upsert : root -> Path.path -> Value.t -> root

  (** Deletes a value from a root. *)
  val delete : root -> Path.path -> root

  (** Delete a list of roots, given by their hash, in the forest. *)
  val gc     : forest -> hash list -> unit

end = struct

  type hash = string
  type index = Stdint.uint32

  type view =
    { node : view_node ; index : index option; hash : hash option }
  and node =
    | View of view
    | Disk of index
    | Null
  and view_node =
      Internal of node * node
    | Bud of Path.segment * node
    | Leaf of Path.segment * Value.t

  type forest =
    {
      array : (char, CamlinternalBigarray.int8_unsigned_elt,
               CamlinternalBigarray.c_layout) Bigarray.Array1.t ;
      mutable length : Stdint.uint32 ;
      leaf_table  : KeyValueStore.t ;
      roots_table : (hash, index) Hashtbl.t
    }

  type root = forest * node

  let empty forest = (forest, Null)


  let array_cell array index =
    let offset = 32 * Stdint.Uint32.to_int index in
    let bytes = Bytes.create 32 in
    for i = offset to offset + 31 do
      Bytes.set bytes i (Bigarray.Array1.get array i)
    done ; bytes

  (* Loads and parse a node from the disk mapped array. *)
  let load_node forest index =
    if index = Stdint.Uint32.zero then Null
    else
      let cell = array_cell forest.array index in
      let marker_bit = ((Bytes.get cell 27 |> int_of_char) land 1 > 0) in
      let child_index = Stdint.Uint32.of_bytes_little_endian cell 28 in
      let previous_index = Stdint.Uint32.(index - one) in

      (* special marker to distinguish leaves from internal nodes *)
      if child_index = Stdint.Uint32.max_int then  (* this is a leaf or a bud *)
        let segment = Bytes.sub cell 0 28 |> Bytes.to_string |> Path.segment_of_string in
        if marker_bit then (* this is just a leaf *)
          let previous_cell = array_cell forest.array previous_index in
          let data_hash = Bytes.sub previous_cell 0 28 in
          match KeyValueStore.get_opt forest.leaf_table (Value.hash_of_bytes data_hash) with
          | None -> Printf.ksprintf failwith "Unknown key %s" (Bytes.to_string data_hash)
          | Some value -> View { node = Leaf (segment, value) ; index = Some index ; hash = Some data_hash }
        else (* this is a bud *)
          View { node = Bud (segment, Disk previous_index) ; index = Some index ; hash = None}
      else (* this is an internal node *)
        let (left, right) =
          if marker_bit then
            (child_index, previous_index)
          else
            (previous_index, child_index) in
        View { node = Internal (Disk left, Disk right); index = Some index; hash = Some (Bytes.sub cell 0 28) }



  let null_hash = Bytes.init 28 (fun _ -> '\000')

  let rec hash_nodes = function
    | Disk index ->
      let n = load_node index in
      hash_nodes n
    | Null -> (Null, null_hash)
    | View { _ ; _ ; Some hash } as n -> (hash, n)
    | View { node ; Some index ; None } as n -> hash_nodes (load_node index) (* should not happen *)
    | View { node ; None ; None } as n ->
      begin
        match node with
        | Internal (left, right) ->
          let (hl, left) = hash_nodes left and (hr, right) = hash_nodes right
          in let hash = node_hash hl hr in
          (hash, View {node = Internal (hl, hr); index = None ; Some hash_nodes}
        | Bud (segment, root) ->
         (Null, null_hash)
          | Leaf _ -> (Null, null_hash)
      end





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
    failwith "not implemented"
 (*
    let offset = 32 * (Stdint.Uint32.to_int index) in
    let b = Bytes.create 32 in
    Bytes.blit b 0 path 0 24;
    Stdint.Uint32.(to_bytes_little_endian (index + one) b 24);
    Stdint.Uint32.(to_bytes_little_endian int_max b 24);
    for i = 0 to 31 do
      Bigarray.Array1.set forest.array (offset+i) (Bytes.get b i)

    (h, Stdint.Uint32.zero) *)

  let commit (forest, node) =
    let rec commit_aux forest node depth =
      match node with
      | Null -> (null_hash, Stdint.Uint32.zero)
      | Disk index -> (get_hash forest.array index, index)
      | View (_, Some index) -> (get_hash forest.array index, index)
      | View (view_node, None) -> begin
          match view_node with
          | Internal (left, right) ->
            let (hleft, ileft)  = commit_aux forest left  (depth + 1)
            and (hright, iright) = commit_aux forest right (depth + 1) in
            let h = mixhash hleft hright
            in (h, write_internal forest ileft iright h)
          | Leaf (path, value) -> write_leaf forest path valeu
        end
    in
    let (hash, index) = commit_aux forest node 0 in
    Hashtbl.add forest.roots_table hash index



  let upsert (forest, node) path value =
    let rec upsert_aux forest path node depth value =
      match (path, node) with

      | ([ ], _) -> failwith "ran out of segments"

      (* Load from disk and re-try *)
      | (_, Disk index) ->
        upsert_aux forest path (load_node forest index) depth value

      (* Null node, insert a leaf directly, or a bud if need be *)
      | ([ segment ] , Null ) -> View {node = Leaf (segment, value); index = None; hash = None}
      | (segment :: rest, Null ) ->
        let bud = View {node = Bud (segment, Null) ; index = None ; hash = None } in
        upsert_aux forest rest bud 0 value

      (* View node, we can actually match on the structure *)
      | (segment :: rest, View {node = view_node ; _  }) -> begin
          match view_node with

          (* An internal node, insert in the right branch *)
          | Internal (left, right) -> begin
              if Path.get_bit segment depth = Left then
                let new_left = upsert_aux forest path node (depth+1) value in
                View {node = Internal (new_left, right) ; index = None ; hash = None}
              else
                let new_right = upsert_aux forest path node (depth+1) value in
                View {node = Internal (left, new_right) ; index = None ; hash = None}
            end

          (* A bud. If paths match, continue inserting deeper, otherwise kick down. *)
          | Bud (bud_segment, next_root) ->
            if segment = bud_segment then (* easy case, we move on to the next segment *)
              upsert_aux forest rest next_root 0 value
            else (* push bud down *)
              let new_bud = View { node = Bud (bud_segment, next_root) ; index = None; hash = None } in
              let internal =
                if Path.get_bit bud_segment depth = Path.Left then
                  View {node = Internal (new_bud, Null) ; index = None ; hash = None}
                else
                  View {node = Internal (Null, new_bud) ; index = None ; hash = None}
              in
              upsert_aux forest path internal depth value

          (* A leaf *)
          | Leaf (other_segment, other_value) ->

            (* same path, update the key *)
            if other_segment = segment then
              if rest = [] then
                View {node = Leaf (segment, value) ; index = None ; hash = None}
              else begin
                Printf.printf "WARNING ERASING A LEAF WITH A BUD" ;
                let bud = View {node = Bud (segment, Null) ; index = None ; hash = None}
                in upsert_aux forest rest bud 0 value
              end

            else (* push down the branch *)
              begin
                let new_leaf = View {node = Leaf (other_segment, other_value); index = None; hash = None } in
                let internal =
                  if Path.get_bit other_segment depth = Path.Left then
                    View {node = Internal (new_leaf, Null) ; index = None ; hash = None}
                  else
                    View {node = Internal (Null, new_leaf); index = None ; hash = None}
                in upsert_aux forest path internal depth value
              end
        end
    in (forest, upsert_aux forest path node 0 value)




  let gc _ _ = ()
  let delete _ _ = failwith "delete not implemented"

  let get_opt _ _ = failwith "get_opt not implemented"

  let create kv_store ~filename =
    let fd = Unix.openfile path ~mode:[Unix.O_WRONLY ; Unix.O_CREAT] in
    let array = Bigarray.array1_of_genarray (
        Unix.map_file fd
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
