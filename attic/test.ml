type foo
type bar

type 'a choice =
  | Foo : foo choice
  | Bar : bar choice

(* DOES NOT compile, because the pattern matching tries to infer from
   the first case that f has type foo choice -> string *)
let f   = function
  | Foo -> "foo"
  | Bar -> "bar"

(* works because we're not trying to do this for all types, merely
   asserting that these types exist *)
let f : type x . x choice -> string = function
  | Foo -> "foo"
  | Bar -> "bar"

(* other syntax *)
let f (type x) (x:x choice) = match x with
  | Foo -> "foo"
  | Bar -> "bar"

(* we could also use an existential wrapper *)

type ex_choice =
  | Choice : 'a choice -> ex_choice

let f = function
  | Choice Foo -> "foo"
  | Choice Bar -> "bar"
