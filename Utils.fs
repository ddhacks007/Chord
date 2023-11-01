module Utils

let inRange entry cid sid m =
    let mutable entry' = entry
    let mutable sid' = sid

    if entry' < cid then
        entry' <- entry' + m
    if sid' < cid then
        sid' <- sid' + m

    cid <= entry' && entry' <= sid'


let dist (m: int) (key1: int) (key2: int): int =
    if key2 > key1 then key2 - key1
    else (m - key1) + key2
    