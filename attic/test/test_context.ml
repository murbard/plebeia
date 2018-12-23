open Context

let test_sha192 () =
  assert (
    sha192 (Bytes.of_string "foo\n")  =
    (Bytes.of_string "\xb5\xbb\x9d\x80\x14\xa0\xf9\xb1\xd6\x1e\x21\xe7\x96\xd7\x8d\xcc\xdf\x13\x52\xf2\x3c\xd3\x28\x12"))

let test_insertion () =
  let table = Data.empty ()
  and value = Data.value_of_string "bar"
  and other = Data.value_of_string "foo" in
  let hash = Data.insert table value in
  let ret = Data.get_opt table hash in
  assert (ret = Some value) ;
  assert (Data.get_opt table (Data.hash other) = None)

let test_refcount () =
  let table = Data.empty ()
  and value = Data.value_of_string "bar" in
  let hash = Data.insert table value in
  for _ = 1 to 5 do
    ignore (Data.insert table value)
  done ;
  for _ = 1 to 5 do
    Data.decr table hash ;
    assert (Data.get_opt table hash = Some value) ;
  done ;
  Data.decr table hash ;
  assert (Data.get_opt table hash = None)

let create_root () =
  let table = Data.empty () in
  let tries = Tries.create ~namespace:"foo/bar" ~leaf_table:table in
  Tries.empty tries

let test_creation () =
  ignore (create_root ())

let test_trie_insert () =
  let root = create_root () in
  let value = Data.value_of_string "baz!" in
  let path = sha192 (Bytes.of_string "123") |> Tries.make_path in
  let root = Tries.upsert root path value in
  ignore root

let test_trie_commit () =
  let root = create_root () in
  let value = Data.value_of_string "baz!" in
  let path = sha192 (Bytes.of_string "123") |> Tries.make_path in
  let root = Tries.upsert root path value in
  Tries.commit root

let data_tests = [
  "test_sha192", `Quick, test_sha192 ;
  "test_insertion", `Quick, test_insertion ;
  "test_refcount", `Quick, test_refcount
]

let tries_tests = [
  "test_creation", `Quick, test_creation ;
  "test_trie_insert", `Quick, test_trie_insert ;
  "test_trie_commit", `Quick, test_trie_commit
]

let () =
  Alcotest.run "test-context" [
    "data", data_tests ;
    "tries", tries_tests
  ]
