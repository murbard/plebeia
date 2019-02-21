open Plebeia_impl

(* What follows is just for debugging purposes, to be removed. *)

open Error

let rec string_of_node : type a b c. (a, b, c) node -> int -> string = fun node indent ->
  (* pretty prints a tree to a string *)
  let indent_string = String.concat "" (List.init indent (fun _ -> " . ")) in
  match node with
    | (Disk (index, _)) -> Printf.sprintf "%sDisk %Ld" indent_string index
    | View (Leaf (value, _, _, _)) ->
      Printf.sprintf "%sLeaf %s\n" indent_string (Value.to_string value)
    | View (Bud  (node , _, _, _)) ->
      let recursive =
        match node with
        | Some node -> string_of_node node (indent + 1)
        | None     ->  "Empty"
      in
      Printf.sprintf "%sBud:\n%s" indent_string recursive
    | View (Internal (left, right, _, _, _)) ->
      Printf.sprintf "%sInternal:\n%s%s" indent_string
        (string_of_node left (indent + 1))
        (string_of_node right (indent + 1))
    | View (Extender (segment, node, _, _, _)) ->
      Printf.sprintf "%s[%s]- %s" indent_string (Path.to_string segment)
        (string_of_node node (indent + 1))

(* Graphviz's dot file format *)
let dot_of_node root =
  let rec aux : type a b c. int -> (a, b, c) node -> (string * string list * int) = fun cntr -> function
    | Disk (index, _) -> 
        let n = Printf.sprintf "Disk%Ld" index in
        (n, [Printf.sprintf "%s [shape=box];" n], cntr)
    | View (Leaf (value, _, _, _)) ->
        let n = Printf.sprintf "Leaf%d\n" cntr in
        (n, [Printf.sprintf "%s [label=%S];" n (Value.to_string value)], cntr+1)
    | View (Bud  (Some node , _, _, _)) ->
        let n', s, cntr = aux cntr node in
        let n = Printf.sprintf "Bud%d" cntr in
        (n, 
         [Printf.sprintf "%s [shape=diamond, label=\"\"];" n;
          Printf.sprintf "%s -> %s;" n n'
         ] @ s,
         cntr + 1)
    | View (Bud  (None , _, _, _)) ->
        let n = Printf.sprintf "Bud%d" cntr in
        (n, 
         [Printf.sprintf "%s [shape=diamond, label=\"\"];" n], 
         cntr + 1)
    | View (Internal (left, right, _, _, _)) ->
        let ln, ls, cntr = aux cntr left in 
        let rn, rs, cntr = aux cntr right in 
        let n = Printf.sprintf "Internal%d" cntr in
        (n,
         [ Printf.sprintf "%s [shape=circle, label=\"\"];" n;
           Printf.sprintf "%s -> %s [label=\"L\"];" n ln;
           Printf.sprintf "%s -> %s [label=\"R\"];" n rn ]
         @ ls @ rs,
         cntr + 1)
    | View (Extender (segment, node, _, _, _)) ->
        let n', s, cntr = aux cntr node in
        let n = Printf.sprintf "Extender%d" cntr in
        (n,
         Printf.sprintf "%s [shape=circle, label=\"\"];" n
         :: Printf.sprintf "%s -> %s [label=%S];" n n' (Path.to_string segment)
         :: s,
         cntr + 1)
  in
  let (_, s, _) = aux 0 root in
  "digraph G {\n" ^ String.concat "\n" s ^ "\n}\n"

(* Graphviz's dot file format *)
let dot_of_cursor c = 
  go_top c >>= function Cursor (_, n, _) -> return @@ dot_of_node n

(* Bud -> Leaf and Bud -> Bud are invalid, but not excluded by the GADT *)
let validate_node context node =
  let rec aux 
    : type a b c . [> `Bud | `Internal | `None] -> context -> (a, b, c) node -> (unit, string) result = 
    fun parent context node ->
    match node with
    | Disk  (i, wit) -> 
        let node = View (load_node context i wit) in
        aux parent context node
    | View v ->
        match v with
        | Leaf _ ->
            begin match parent with
            | `None -> Error "Leaf cannot be the root"
            | `Bud -> Error "Leaf cannot have a Bud as the parent"
            | `Internal | `Extender -> Ok ()
            end
        | Bud (child, _, _, _) ->
            begin match parent with
            | `Bud -> Error "Bud cannot have a Bud as the parent"
            | `Internal | `Extender | `None ->
                match child with
                | None -> Ok ()
                | Some node -> aux `Bud context node
            end
        | Internal (n1, n2, _, _, _) ->
            begin match parent with
            | `None -> Error "Internal cannot be the root"
            | `Bud | `Internal | `Extender ->
                aux `Internal context n1 >>= fun () ->
                aux `Internal context n2
            end
        | Extender (seg, node, _, _, _) ->
            if List.length (seg :> Path.side list) > 223 then 
              Error "segment too long"
            else begin 
              match parent with
              | `None -> Error "Extender cannot be the root"
              | `Extender -> 
                  (* This one should be checked already by the GADT *)
                  Error "Extender cannot have an Extender as the parent"
              | `Bud | `Internal -> aux `Extender context node
              end
  in
  aux `None context node
