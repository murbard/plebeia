open Plebeia.Plebeia_impl
module P = Plebeia.Plebeia_impl
  
(* unoptimized tree *)
type t = 
  | Null
  | Leaf of Value.t
  | Tree of t
  | Node of t * t
    
type trail = Root | Treed of trail | Left of t * trail | Right of t * trail

type segment = Plebeia.Plebeia_impl.Path.side list
type error = string
type value = Plebeia.Plebeia_impl.value
type context = unit

type cursor = t * trail

let get_root_node (t, _) = t
  
let rec of_plebeia_node : type a b c . P.context -> (a,b,c) P.node -> t = fun context -> function
  | Disk (i, wit) -> of_plebeia_node context (View (load_node context i wit))
  | View n  -> 
      match n with
      | Bud (None, _, _, _) -> Tree Null
      | Bud (Some n, _, _, _) -> Tree (of_plebeia_node context n)
      | Internal (l, r, _, _, _) -> Node (of_plebeia_node context l,
                                          of_plebeia_node context r)
      | Leaf (v, _, _, _) -> Leaf v
      | Extender (seg, n, a, b, c) ->
          match Path.cut seg with
          | None -> of_plebeia_node context n
          | Some (Path.Left, seg) -> Node (of_plebeia_node context (View (Extender (seg, n, a, b, c))), Null)
          | Some (Path.Right, seg) -> Node (Null, of_plebeia_node context (View (Extender (seg, n, a, b, c))))
  
let empty () = (Tree Null, Root) (* not just Null *)
  
(*
let rec go_down (n, trail) dir = match n with
  | Null -> Error "Null"
  | Leaf _ -> Error "Leaf"
  | Tree t -> go_down (t, Treed trail) dir
  | Node (l, r) ->
      match dir with
      | `Left -> Ok (l, Left (r, trail))
      | `Right -> Ok (r, Right (l, trail))
*)

(*
let rec access ((n, trail) as cur) seg = 
  match seg with
  | [] -> Ok cur
  | Path.Left :: seg' ->
      begin match n with
      | Null -> Error "Null"
      | Leaf _ -> Error "Leaf"
      | Tree n -> access (n, Treed trail) seg
      | Node (l, r) -> access (l, Left (r, trail)) seg'
      end
  | Path.Right :: seg' ->
      begin match n with
      | Null -> Error "Null"
      | Leaf _ -> Error "Leaf"
      | Tree n -> access (n, Treed trail) seg
      | Node (l, r) -> access (r, Right (l, trail)) seg'
      end

let go_up (n, trail) = 
  match trail with
  | Root -> Error "Root"
  | Treed trail -> Ok (Tree n, trail)
  | Left (r, trail) -> Ok (Node (n, r), trail)
  | Right (l, trail) -> Ok (Node (l, n), trail)
*)

let check_node (n, trail) =
  match n with
  | Tree n -> Ok (n, Treed trail)
  | _ -> Error "Start node is not Tree"

let subtree ntrail seg =
  let rec aux ((n, trail) as cur) = function
    | [] -> 
        begin match n with
        | Tree _ -> Ok cur
        | _ -> Error "Reached to non Tree"
        end
    | Path.Left :: seg' ->
        begin match n with
        | Null -> Error "Null"
        | Leaf _ -> Error "Leaf"
        | Tree _ -> Error "Tree in middle"
        | Node (l, r) -> aux (l, Left (r, trail)) seg'
        end
    | Path.Right :: seg' ->
        begin match n with
        | Null -> Error "Null"
        | Leaf _ -> Error "Leaf"
        | Tree _ -> Error "Tree in middle"
        | Node (l, r) -> aux (r, Right (l, trail)) seg'
        end
  in
  let open Error in
  check_node ntrail >>= fun ntrail -> aux ntrail seg

(* Find the Tree above, cleaning Node (Null, Null) *)
let rec go_up_node (n, trail) =
  let trim_null = function
    | Node (Null, Null) -> Null
    | t -> t
  in
  match trail with
  | Treed trail -> Ok (Tree n, trail)
  | Root -> Error "Root"
  | Left (r, trail) -> go_up_node (trim_null (Node (n,r)), trail)
  | Right (l, trail) -> go_up_node (trim_null (Node (l, n)), trail)
  
let parent ((n, _) as ntrail) =
  match n with
  | Tree _ -> go_up_node ntrail
  | _ -> Error "not Tree"
  
let get_node ntrail seg =
  let rec aux ((n, trail) as ntrail) = function
    | [] -> Ok ntrail
    | Path.Left :: seg' ->
        begin match n with
        | Null -> Error "Null"
        | Leaf _ -> Error "Leaf"
        | Tree _ -> Error "Tree in middle"
        | Node (l, r) -> aux (l, Left (r, trail)) seg'
        end
    | Path.Right :: seg' ->
        begin match n with
        | Null -> Error "Null"
        | Leaf _ -> Error "Leaf"
        | Tree _ -> Error "Tree in middle"
        | Node (l, r) -> aux (r, Right (l, trail)) seg'
        end
  in
  let open Error in
  check_node ntrail >>= fun ntrail -> 
  aux ntrail seg

let get ntrail seg =
  let open Error in
  get_node ntrail seg >>= function
  | (Leaf v, _) -> Ok v
  | _ -> Error "Not Leaf"

let alter ntrail seg f =
  let open Error in
  let rec aux (n, trail) = function
    | [] -> f n >>= fun v -> Ok (v, trail)
    | Path.Left :: seg' ->
        begin match n with
        | Null -> aux (Null, Left (Null, trail)) seg'
        | Leaf _ -> Error "Leaf"
        | Tree _ -> Error "Tree in middle"
        | Node (l, r) -> aux (l, Left (r, trail)) seg'
        end
    | Path.Right :: seg' ->
        begin match n with
        | Null -> aux (Null, Right (Null, trail)) seg'
        | Leaf _ -> Error "Leaf"
        | Tree _ -> Error "Tree in middle"
        | Node (l, r) -> aux (r, Right (l, trail)) seg'
        end
  in
  let open Error in
  check_node ntrail >>= fun ntrail -> 
  aux ntrail seg >>= go_up_node

let insert ntrail seg v = 
  let f = function
    | Null -> Ok (Leaf v)
    | _ -> Error "not Null"
  in
  alter ntrail seg f
    
let upsert ntrail seg v = 
  let f = function
    | Null | Leaf _ -> Ok (Leaf v)
    | _ -> Error "not Null nor Leaf"
  in
  alter ntrail seg f
    
let create_subtree ntrail seg = 
  let f = function
    | Null -> Ok (Tree Null)
    | _ -> Error "not Null"
  in
  alter ntrail seg f 

let delete ntrail seg = 
  let open Error in
  get_node ntrail seg >>= function
  | ((Leaf _ | Tree _), trail) -> go_up_node (Null, trail)
  | _ -> Error "Not Leaf nor Tree"

(* Graphviz's dot file format *)
let dot_of_node root =
  let rec aux cntr = function
    | Null ->
        let n = Printf.sprintf "Null%d\n" cntr in
        (n, [Printf.sprintf "%s [shape=point];" n],
         cntr + 1)

    | Leaf value ->
        let n = Printf.sprintf "Leaf%d\n" cntr in
        (n, [Printf.sprintf "%s [label=%S];" n (Value.to_string value)], cntr+1)
        
    | Tree Null ->
        let n = Printf.sprintf "Bud%d" cntr in
        (n, 
         [Printf.sprintf "%s [shape=diamond, label=\"\"];" n], 
         cntr + 1)

    | Tree t ->
        let n', s, cntr = aux cntr t in
        let n = Printf.sprintf "Bud%d" cntr in
        (n, 
         [Printf.sprintf "%s [shape=diamond, label=\"\"];" n;
          Printf.sprintf "%s -> %s;" n n'
         ] @ s,
         cntr + 1)
    | Node (left, right) -> 
        let ln, ls, cntr = aux cntr left in 
        let rn, rs, cntr = aux cntr right in 
        let n = Printf.sprintf "Internal%d" cntr in
        (n,
         [ Printf.sprintf "%s [shape=circle, label=\"\"];" n;
           Printf.sprintf "%s -> %s [label=\"L\"];" n ln;
           Printf.sprintf "%s -> %s [label=\"R\"];" n rn ]
         @ ls @ rs,
         cntr + 1)
  in
  let (_, s, _) = aux 0 root in
  "digraph G {\n" ^ String.concat "\n" s ^ "\n}\n"

let dot_of_cursor (t, _) = dot_of_node t

