
module Path : sig
  type direction = Left | Right
  type 'a segment = private direction list
  type internal
  type leaf
  type full
  val fit_in_internal : 'a segment -> bool
  val fit_in_leaf : 'a segment -> bool
  val first : 'a segment -> direction option
  val empty : 'a segment -> bool
  val to_leaf : full segment  -> leaf segment
  val extract_internal : full segment  -> (internal segment) * (full segment)
  val first_direction : 'a segment -> direction
  val common_prefix : 'a segment -> 'b segment -> internal segment * 'a segment * 'b segment
  val refill : internal segment -> 'a segment -> (internal segment * 'a segment)
  val of_direction_list : direction list -> full segment
  val print : 'a segment -> string
  val concat : 'a segment -> 'b segment -> full segment
  val balance_internal_and_leaf : internal segment -> leaf segment -> (internal segment * leaf segment)
  val len : 'a segment -> int
end = struct
  type internal
  type leaf
  type full
  type direction = Left | Right
  type 'a segment = direction list

  let len = List.length

  let internal_length = 16
  let leaf_length = 208

  let concat x y = x @ y

  let print s = Printf.sprintf "{%s}" (String.concat ", " (List.map (function | Left -> "left" | Right -> "rite") s))
  let fit_in_internal s = List.length s <= internal_length
  let fit_in_leaf s = List.length s <= leaf_length
  let empty s = s = []
  let first = function
    | [] -> None
    | h::_ -> Some h

  let of_direction_list l = l



  let to_leaf segment = assert (fit_in_leaf segment); segment
  let extract_internal segment =
    let rec aux (internal, seg) count =
      if count = 0 then
        (internal, seg)
      else match seg with
        | [] -> (internal, [])
        | h :: t -> let (internal, seg) = aux (internal, t) (count-1) in
          (h::internal, seg)
    in aux ([], segment) internal_length


  let balance_internal_and_leaf internal_seg leaf_seg =
    let both = internal_seg @ leaf_seg in
    if List.length both <= leaf_length then
      ([], both)
    else
      extract_internal both


  let first_direction s = List.hd s

  let  common_prefix l1 l2 =
    let rec cp_aux l1 l2 len =
      if len = internal_length then
        ([], l1, l2)
      else
        match (l1, l2) with
        | ([], []) -> ([], [], [])
        | (l1, []) -> ([], l1, [])
        | ([], l2) -> ([], [], l2)
        | (h1::t1, h2::t2) ->
          if h1 = h2 then
            let (lcp, r1, r2) = cp_aux t1 t2 (len+1) in
            (h1 :: lcp, r1, r2)
          else
            ([], l1, l2)
    in let (cp, r1, r2) = cp_aux l1 l2 0 in
    if cp = [] then begin
      Printf.printf "oh la la\n       %s\n       %s\n" (print l1) (print l2) ;
      failwith "no common prefix"
    end
    else
      (cp, r1, r2)

  let refill internal_segment segment =
    let rec aux internal_segment segment count =
      if count = 0 then ([], segment)
      else
        match internal_segment with
        | h :: t -> let (i, s) = aux t segment (count - 1) in
          (h :: i, s)
        | [ ] -> begin
            match segment with
            | h :: t -> let (i, s) = aux internal_segment t (count - 1) in
              ( h :: i, s)
            | [ ] -> ([], [])
          end
    in aux internal_segment segment internal_length

end

type value = string

type node =
  | Internal of (Path.internal Path.segment) * node * node
  | Leaf of (Path.leaf Path.segment) * value
  | Null

type root = node * node

let rec reduce node =
  match node with
  | Internal (_, Null, Null) -> Null
  | Internal (internal_segment, x, Null) -> reduce_internal internal_segment x
  | Internal (internal_segment, Null, x) -> reduce_internal internal_segment x
  | _ -> node

and reduce_internal internal_segment node =
  match node with
  | Null -> node
  | Leaf (leaf_segment, value) ->
    (* maybe the internal segment fits in the leaf now *)
    let (internal_segment, leaf_segment) = Path.balance_internal_and_leaf internal_segment leaf_segment in
    let leaf = Leaf (leaf_segment, value) in
    assert (not (Path.empty leaf_segment));
    begin
      match (Path.first internal_segment, Path.first leaf_segment) with
      | (None, Some _) -> leaf
      | (Some _, Some leaf_side) -> Internal (
          internal_segment,
          (if leaf_side = Path.Left then leaf else Null),
          (if leaf_side = Path.Right then leaf else Null))
      | _ -> assert false
    end
  | Internal (next_internal_segment, left, right) ->
    let (internal_segment, next_internal_segment) = Path.refill internal_segment next_internal_segment in
    match Path.first next_internal_segment with
    | None ->  reduce (Internal (internal_segment, left, right))
    | Some Right -> Internal (internal_segment, Null, reduce (Internal (next_internal_segment, left, right)))
    | Some Left ->  Internal (internal_segment, reduce (Internal (next_internal_segment, left, right)), Null)



let rec print_node node indent max_indent =
  if indent > max_indent then "" else begin
    let id = String.concat "" (List.init indent (fun _ -> " → ")) in
    match node with
    | Null -> Printf.sprintf "%snull" id
    | Leaf (p, v) -> Printf.sprintf "%sLeaf %s : %s" id (Path.print p) v
    | Internal (p, l, r) -> Printf.sprintf "%sInternal %s : (\n%s,\n%s)" id (Path.print p) (print_node l (indent+1) max_indent) (print_node r (indent+1) max_indent)
  end


let return_internal segment result b side =
  match result with
  | Ok a -> begin
      match side with
      | Path.Left -> Ok (Internal (segment, a, b))
      | Path.Right -> Ok (Internal (segment, b, a))
    end
  | Error (seg1, seg2) ->
    Error (Path.concat segment seg1,
           Path.concat segment seg2)


let rec insert root segment value =

  (*  Printf.printf "Inserting\n%s\ninto\n%s\nfor %s\n" (Path.print segment) (print_node root 0 10) value; *)
  (* Convention : when inserting in an internal node or into a leaf, the segment
     associated with the leaf or the node has *not* been absorbed yet.
     The segment's first element is the last direction taken. *)
  match root with
  | Null ->
    if Path.fit_in_leaf segment then
      (* If there are 64 or fewer bits left in the segment, we can store it
         in the leaf. *)
      let leaf_segment = Path.to_leaf segment  in
      assert (not (Path.empty leaf_segment));
      Ok (Leaf (leaf_segment, value))
    else
      (* Otherwise, we need to create an intermediate node. *)
      let (internal_segment, segment) = Path.extract_internal segment in begin
        match Path.first segment with
        | None -> assert false (* we would have fitted in a leaf if that were the case *)
        | Some direction ->  return_internal internal_segment (insert Null segment value) Null direction
      end

  | Leaf (other_leaf_segment, other_value) ->
    (* For a leaf, we need to create and intermediary node and push the leaf down *)

    (* First we compute the  common prefix between the two paths. It should have
       at least one element, the direction we came from. *)
    let (internal_segment, segment_cut, other_leaf_segment_cut) = Path.common_prefix segment other_leaf_segment in
    let other_leaf = Leaf (other_leaf_segment_cut, other_value) in
    begin
      match (Path.first segment_cut, Path.first other_leaf_segment_cut)  with
      | (None, None) -> Ok (Leaf (other_leaf_segment, value)) (* the leaves match: upsert *)
      | (None, Some _)    (* the internal segment takes us home but there's stuff left for the other leaf! *)
      | (Some _, None) -> (* or on the contrary we still have work to do but the leaf is aprefix of ours *)
        Error (Path.concat internal_segment segment_cut, Path.concat internal_segment other_leaf_segment_cut)
      | (Some side, Some other_side) ->
        if side = other_side then
          return_internal internal_segment (insert other_leaf segment_cut value) Null side
        else
          return_internal internal_segment (insert Null segment_cut value) other_leaf side
    end

  (* I meet an internal node, I need to get a common prefix with its segment and then
     break down or insert *)
  | Internal (other_internal_segment, left, right) ->
    let (internal_segment, segment, other_internal_segment) = Path.common_prefix segment other_internal_segment in
    begin
      match (Path.first segment, Path.first other_internal_segment) with
      (* If we've absorbed all of the segment *)
      | (Some Left, None)  ->  return_internal internal_segment (insert left segment value) right Left
      | (Some Right, None) ->  return_internal internal_segment (insert right segment value) left Right

      (* we can't have nothing left *)
      | (None, _) -> Error (Path.concat internal_segment segment,
                            Path.concat internal_segment other_internal_segment)

      | (Some side, Some other_side) ->
          let new_internal = reduce (Internal (other_internal_segment, left, right)) in
          if (side = other_side) then
            return_internal internal_segment (insert new_internal segment value) Null side
          else
            return_internal internal_segment (insert Null segment value) new_internal side
    end

let insert_in (node_left, node_right) path value =
  let direction = Path.first_direction path in
  if direction = Left then
    match insert node_left path value with
    | Ok node -> Ok (node, node_right)
    | Error e -> Error e
  else
    match insert node_right path value with
    | Ok node -> Ok (node_left, node)
    | Error e -> Error e

let rec rand_path n =
  if n = 0 then []
  else  (if Random.bool () then Path.Left else Path.Right) :: (rand_path (n-1))


let rec sum_lists l1 l2 = match (l1, l2) with
  | ([], r) -> r
  | (r, []) -> r
  | (h1::t1, h2::t2) -> (h1+h2) :: (sum_lists t1 t2)

let rec histogram = function
  | Null -> [0]
  | Leaf _ -> [1]
  | Internal (_, l, r) ->
    let hl = 0 ::  (histogram l)
    and hr = 0 ::  (histogram r)
    in sum_lists hl hr

let print_histo root = List.mapi (fun i x -> Printf.printf "%d:\t %d\n" i x) (histogram root) |> ignore



let rec find_error node count =
  if count = 0 then
    (node, Path.of_direction_list [])
  else begin
    if count  mod 100000 = 0 then begin
      Printf.printf "%d...\n" count ; flush stdout ; end
    else ();
    let path = Path.of_direction_list (Path.Left :: (rand_path 206))
    and v  = (Random.int 1000 |> string_of_int) in
    match (try Some (insert node path v) with _ -> None) with
    | Some (Ok node) -> find_error node (count - 1 )
    | Some (Error (s1, s2)) ->
      Printf.printf "Shit an error!\n%s\n%s\n" (Path.print s1) (Path.print s2);
      failwith "Shit an error!"
    | None -> (node, path)
  end
