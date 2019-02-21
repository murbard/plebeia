open Plebeia_impl

val string_of_node : ('a, 'b, 'c) node -> int (* indent *) -> string

val dot_of_node : ('a, 'b, 'c) node -> string
(** Obtain Graphviz dot file representation of the tree *)

val dot_of_cursor : cursor -> (string, error) result

val validate_node : context -> ('a, 'b, 'c) node -> (unit, string) result
