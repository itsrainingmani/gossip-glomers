host := `uname -a`

serve:
  ./maelstrom/maelstrom serve

# challenge 1: echo
echo:
  go install . && ./maelstrom/maelstrom test -w echo --bin ~/go/bin/gossglom --node-count 1 --time-limit 10

# challenge 2: unique id
unique:
  go install . && ./maelstrom/maelstrom test -w unique-ids --bin ~/go/bin/gossglom --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

# challenge 3a: single node broadcast
single-broad:
  go install . && ./maelstrom/maelstrom test -w broadcast --bin ~/go/bin/gossglom --node-count 1 --time-limit 20 --rate 10

# challenge 3b: multi node broadcast
multi-broad:
  go install . && ./maelstrom/maelstrom test -w broadcast --bin ~/go/bin/gossglom --node-count 5 --time-limit 20 --rate 10

# challenge 3c: fault tolerant broadcast
faulty-broad:
  go install . && ./maelstrom/maelstrom test -w broadcast --bin ~/go/bin/gossglom --node-count 5 --time-limit 20 --rate 10 --nemesis partition
