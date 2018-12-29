type hashed
type not_hashed

type ('ha, 'hb, 'hc) transitive =
  | All : string -> (hashed, hashed, hashed) transitive
  | NotAll : (not_hashed, 'ib, 'ic) transitive

type 'h node =
  | Leaf : hashed node
  | Node : 'hl node * 'hr node * ('h, 'hl, 'hr) transitive -> 'h node
  | Food : {i : 'h} -> 'h node

type 'trail trail =
  | Top :  'hole trail
  | Left : ('phole * 'ptrail) trail * 'hr node * ('phole, 'hole, 'hr) transitive ->
    ('hole * ('phole * 'ptrail)) trail
  | Right : 'hl node * ('phole * 'ptrail) trail * ('phole, 'hl, 'hole) transitive ->
    ('hole * ('phole * 'ptrail)) trail

type ('hole, 'prev) cursor = ('hole * 'prev) trail * 'hole node

let up x = match x with
  | (Top, _) ->  failwith "can't go up"
  | (Left (trail, right, transitive), node) -> (trail, Node (node, right, transitive))
  | (Right (left, trail, transitive), node ) -> (trail, Node (left, node, transitive))
