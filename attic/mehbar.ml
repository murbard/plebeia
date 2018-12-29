type hashed
type not_hashed

type ('ha, 'hb, 'hc) transitive =
  | All : string -> (hashed, hashed, hashed) transitive
  | NotAll : (not_hashed, 'ib, 'ic) transitive

type 'h node =
  | Leaf : hashed node
  | Node : 'hl node * 'hr node * ('h, 'hl, 'hr) transitive -> 'h node

let tree = Node (Node (Leaf, Leaf, All "foo"), Node (Leaf, Leaf, NotAll), NotAll )

type 'hole trail =
  | Top
  | Left :  'ht trail * 'hr node * ('ht, 'hole, 'hr) transitive -> 'hole trail
  | Right : 'hl node * 'ht trail * ('ht, 'hl, 'hole) transitive -> 'hole trail

type cursor = Cursor : 'h trail * ('h node) -> cursor

let rec up :  cursor -> cursor =
  fun cursor -> match cursor with
    | Cursor (Top, _) -> cursor (*can't go up*)
    | Cursor (Left (trail, right, transitive), node) -> Cursor (trail, Node (node, right, transitive))
    | Cursor (Right (left, trail, transitive), node) -> Cursor (trail, Node (left, node, transitive))
