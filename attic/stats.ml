type node =
  | Node of node * node
  | Leaf of int
  | Null


let rec insert node path  =
  match node with
  | Null -> Leaf path
  | Leaf p -> insert (insert (Node (Null, Null)) path)  p
  | Node (l, r) -> let h = (path mod 2 = 0) and rest = (path / 2) in if h then Node (l, insert r rest) else Node(insert l rest, r)


let rand_path () =
  let x = ref 0 in
  for i = 0 to 62 do
    x := 2 * !x;
    if Random.bool () then
      x := !x + 1
    else () ;
  done; !x


let tree c =
  let n = ref Null in
  for i = 0 to c do
    n := insert !n (rand_path ())
  done; !n

type count = {null : int ; leaf : int ; node : int }

let rec count = function
  | Null -> { null = 1 ; leaf = 0 ; node = 0 }
  | Leaf _ -> { null = 0 ; leaf = 1; node = 0 }
  | Node (l, r) -> let cl = (count l) and cr = (count r)
    in { null = cl.null + cr.null ; leaf = cl.leaf + cr.leaf ; node = cl.node + cr.node + 1}

let rec depth = function
  | Null -> 0
  | Leaf _ -> 0
  | Node (l, r) -> 1 + max (depth l) (depth r)


let avg_depth root =
  let rec avg_depth_aux = function
    | Null -> (0, 0)
    | Leaf  _ -> (1, 0)
    | Node (l, r) ->
      let (cl, tl) = avg_depth_aux l
      and (cr, tr) = avg_depth_aux r in
      (cl + cr, cl + cr + tl + tr)
  in let (c, t) = avg_depth_aux root in
  (float_of_int t) /. (float_of_int c)

let rec sum_lists l1 l2 = match (l1, l2) with
  | ([], r) -> r
  | (r, []) -> r
  | (h1::t1, h2::t2) -> (h1+h2) :: (sum_lists t1 t2)

let rec histogram = function
  | Null -> [0]
  | Leaf _ -> [1]
  | Node (l, r) ->
    let hl = 0 ::  (histogram l)
    and hr = 0 ::  (histogram r)
    in sum_lists hl hr




let rec compact = function
  | Null -> Null
  | Leaf p -> Leaf p
  | Node (l, Null) -> compact l
  | Node (Null, r) -> compact r
  | Node (l, r) -> Node (compact l, compact r)
