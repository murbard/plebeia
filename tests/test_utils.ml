open Plebeia.Plebeia_impl

let timed f = 
  let t1 = Unix.gettimeofday () in
  let res = Error.protect f in
  let t2 = Unix.gettimeofday () in
  (res, t2 -. t1)

let random_segment ?length st =
  let open Path in
  let open Random.State in
  let length = 
    match length with
    | None -> int st 222 + 1 (* 1..223 *)
    | Some l -> l
  in
  let rec f = function
    | 0 -> []
    | n -> (if bool st then Left else Right) :: f (n-1)
  in
  of_side_list @@ f length

let from_Some = function
  | Some x -> x
  | _ -> failwith "Not Some"

let to_file fn s =
  let oc = open_out fn in
  output_string oc s;
  close_out oc

let shuffle st xs =
  let a = Array.of_list xs in
  let size = Array.length a in
  for i = 0 to size - 2 do
    let pos = Random.State.int st (size - i - 1) + i in
    let x = Array.unsafe_get a pos in
    Array.unsafe_set a pos @@ Array.unsafe_get a i;
    Array.unsafe_set a i x
  done;
  Array.to_list a

let test_with_context f =
  let context =
    let fn = Filename.temp_file "plebeia" "test" in
    make_context ~shared:true ~length:1024 fn
  in
  let cursor = empty context in
  let res = f cursor in
  free_context context;
  res
    
let path_of_string s = from_Some @@ Path.of_string s

    
