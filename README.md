Ebolt
=====
Ebolt is lightweight Erlang driver for Neo4j BOLT protocol.

Usage
===============

```erl

{ok, C} = ebolt:connect([{username, <<"neo4j">>}, {password, <<>>}, {host, "localhost"}, {port, 7687}]).

ebolt:run(C, <<"CREATE (a:Person {name:'Bob'}) RETURN a">>, #{}).

```
