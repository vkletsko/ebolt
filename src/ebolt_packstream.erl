-module(ebolt_packstream).
-author("Kletsko Vitali <v.kletsko@gmail.com>").

%% API
-export([
  encode/1,
  decode/1,

  %% utils
  msg_to_bin/2,
  encode_map/2
  ]).

%% Protocol description
%% http://boltprotocol.org/
%%%===================================================================
%%% Decode
%%%===================================================================
-spec decode(Bin:: binary()) -> [any()].
%% null
decode(<<16#C0, Rest/binary>>) ->
  [null | decode(Rest)];

%% boolean
decode(<<16#C3, Rest/binary>>) ->
  [true | decode(Rest)];
decode(<<16#C2, Rest/binary>>) ->
  [false | decode(Rest)];

%% float
decode(<<16#C1, Number/float, Rest/binary>>) ->
  [Number | decode(Rest)];

%% strings
decode(<<16#8:4, Len:4, Rest/binary>>) ->
  decode_str(Rest, Len);
decode(<<16#D0, Len:8, Rest/binary>>) ->
  decode_str(Rest, Len);
decode(<<16#D1, Len:16/big-unsigned-integer, Rest/binary>>) ->
  decode_str(Rest, Len);
decode(<<16#D2, Len:32/big-unsigned-integer, Rest/binary>>) ->
  decode_str(Rest, Len);

%% Lists
decode(<<16#9:4, ListSize:4, Rest/binary>>) ->
  decode_list(Rest, ListSize);
decode(<<16#D4, ListSize:8, Rest/binary>>) ->
  decode_list(Rest, ListSize);
decode(<<16#D5, ListSize:16/big-unsigned-integer, Rest/binary>>) ->
  decode_list(Rest, ListSize);
decode(<<16#D6, ListSize:32/big-unsigned-integer, Rest/binary>>) ->
  decode_list(Rest, ListSize);
%% List Stream
decode(<<16#D7, Bin/binary>>) ->
  Bytes = [Byte || << Byte >> <= Bin],
  ListBitSize = index_of(16#DF, Bytes),
  <<List:ListBitSize/binary, 16#DF, Rest/binary>> = Bin,
  [decode(List) | decode(Rest)];

%% Map
decode(<<16#A:4, Entries:4, Rest/binary>>) ->
  decode_map(Rest, Entries);
decode(<<16#D8, Entries:8, Rest/binary>>) ->
  decode_map(Rest, Entries);
decode(<<16#D9, Entries:16/big-unsigned-integer, Rest/binary>>) ->
  decode_map(Rest, Entries);
decode(<<16#DA, Entries:32/big-unsigned-integer, Rest/binary>>) ->
  decode_map(Rest, Entries);
%% Map Stream
decode(<<16#DB, Bin/binary>>) ->
  Bytes = [Byte || <<Byte>> <= Bin],
  MapBitSize = index_of(16#DF, Bytes),
  <<Map:MapBitSize/binary, 16#DF, Rest/binary>> = Bin,
  [to_map(decode(Map)) | decode(Rest)];

%% Struct
decode(<<16#B:4, StructSize:4, Sig:8, Rest/binary>>) ->
  decode_struct(Rest, Sig, StructSize);
decode(<<16#DC, StructSize:8, Sig:8, Rest/binary>>) ->
  decode_struct(Rest, Sig, StructSize);
decode(<<16#DD, StructSize:16/big-unsigned-integer, Sig:8, Rest/binary>>) ->
  decode_struct(Rest, Sig, StructSize);

%% Integers
decode(<<16#C8, Int:8/big-signed-integer, Rest/binary>>) ->
  [Int|decode(Rest)];
decode(<<16#C9, Int:16/big-signed-integer, Rest/binary>>) ->
  [Int|decode(Rest)];
decode(<<16#CA, Int:32/big-signed-integer, Rest/binary>>) ->
  [Int|decode(Rest)];
decode(<<16#CB, Int:64/big-signed-integer, Rest/binary>>) ->
  [Int|decode(Rest)];
decode(<<Int, Rest/binary>>) ->
%%  error_logger:info_msg("works like int: ~p~n", [Int, Rest]),
  [Int|decode(Rest)];

%% Empty
decode(<< 0, 0 >>) -> [];
decode(<<>>) -> [].

%% @Todo
decode_str(<<>>, _Len) -> [<<>>];
decode_str(Bin, Len) ->
  <<String:Len/binary, Rest/binary >> = Bin,
  [String | decode(Rest)].

%% @Todo
decode_list(Bin, ListSize) ->
  {List, Rest} = lists:split(ListSize, decode(Bin)),
  [List | Rest].

%% @Todo
decode_map(Bin, Entries) ->
  Decoded = decode(Bin),
  {Map, Rest} = lists:split(Entries * 2, Decoded),
  [to_map(Map) | Rest].

decode_struct(Bin, Sig, StructSize) ->
  Decoded = decode(Bin),
  {Struct, Rest1} = lists:split(StructSize, Decoded),
  [[{sig, Sig}, {fields, Struct}]| Rest1].

%%%===================================================================
%%% Encode
%%%===================================================================
-spec encode(any()) -> binary().
%% Atoms
encode(null) -> << 16#C0 >>;
encode(true) -> << 16#C3 >>;
encode(false) -> << 16#C2 >>;
encode(Other) when is_atom(Other) ->
  encode(erlang:atom_to_binary(Other, utf8));
%% float
encode(Float) when is_float(Float) ->
  <<16#C1, Float/float>>;

%% Ints
%%@int8 -127..-17
%%@int16_low  -32_768..-129
%%@int16_high 128..32_767
%%@int32_low  -2_147_483_648..-32_769
%%@int32_high 32_768..2_147_483_647
%%@int64_low  -9_223_372_036_854_775_808..-2_147_483_649
%%@int64_high 2_147_483_648..9_223_372_036_854_775_807

encode(Int) when is_integer(Int)
  andalso (Int >= -16 andalso Int =< 127) ->   %% Tiny Int
  << Int >>;
encode(Int) when is_integer(Int)
  andalso (Int >= -128 andalso Int =< -17) ->
  << 16#C8, Int:8/big-signed-integer >>;
encode(Int) when is_integer(Int)
  andalso ((Int >= -32768 andalso Int =< -129)
    orelse (Int >= 128 andalso Int =< 32767)) ->
  << 16#C9, Int :16/big-signed-integer >>;
encode(Int) when is_integer(Int)
  andalso ((Int >= -2147483648 andalso Int =< -32769)
    orelse (Int >= 32768 orelse Int =< 2147483647)) ->
  << 16#CA, Int:32/big-signed-integer >>;
encode(Int) when is_integer(Int)
  andalso ((Int >= -9223372036854775808 andalso Int =< -2147483649)
    orelse (Int >= 2147483648 andalso Int =< 9223372036854775807)) ->
<< 16#CB, Int:64/big-signed-integer >>;

%% Strings
encode(BinStr) when is_binary(BinStr) ->
  encode_str(BinStr, byte_size(BinStr));

%% Lists
encode(List) when is_list(List) ->
  F = fun (Elem, Acc) -> <<Acc/binary, (encode(Elem))/binary>> end,
  Bin = lists:foldl(F, <<>>, List),
  encode_list(Bin, length(List));

%% Map
encode(Map) when is_map(Map) ->
  encode_map(Map, map_size(Map)).

encode_str(Str, Size) when Size =< 15 ->
  << 16#8:4, Size:4, Str/binary>>;
encode_str(Str, Size) when Size =< 255 ->
  << 16#D0, Size:8, Str/binary>>;
encode_str(Str, Size) when Size =< 65535 ->
  << 16#D1, Size:16, Str/binary>>;
encode_str(Str, Size) when Size =< 4294967295 ->
  << 16#D2, Size:32, Str/binary>>.

encode_list(Bin, ListSize) when ListSize =< 15 ->
  << 16#9:4, ListSize:4, Bin/binary>>;
encode_list(Bin, ListSize) when ListSize =< 255 ->
  << 16#D4, ListSize:8, Bin/binary >>;
encode_list(Bin, ListSize) when ListSize =< 65535 ->
  << 16#D5, ListSize:16/big-unsigned-integer, Bin/binary >>;
encode_list(Bin, ListSize) when ListSize =< 4294967295 ->
  << 16#D6, ListSize:32/big-unsigned-integer, Bin/binary >>;
encode_list(Bin, _ListSize) ->
  << 16#D7, Bin/binary, 16#DF>>.

encode_map(Map, Size) when Size =< 15 ->
  << 16#A:4, Size:4, (encode_map_kv(Map))/binary >>;
encode_map(Map, Size) when Size =< 255 ->
  << 16#D8, Size:8, (encode_map_kv(Map))/binary >>;
encode_map(Map, Size) when Size =< 65535 ->
  << 16#D9, Size:16/big-unsigned-integer, (encode_map_kv(Map))/binary>>;
encode_map(Map, Size) when Size =< 4294967295 ->
  << 16#DA, Size:32/big-unsigned-integer, (encode_map_kv(Map))/binary>>.


encode_map_kv(Map) ->
  Fun = fun (Key, Val) -> << (encode(Key))/binary, (encode(Val))/binary >> end,
  map_to_bin(Fun, Map).

map_to_bin(EncodeFun, Map) ->
  Fun = fun (K, V, Acc) -> << Acc/binary, (EncodeFun(K, V))/binary >> end,
  maps:fold(Fun, <<>>, Map).

%% -----------------------------------------------------------------------
%% utils
%% -----------------------------------------------------------------------
-spec msg_to_bin(fun(), list()) -> binary().
msg_to_bin(EncodeFun, List) ->
  Fun = fun (Elem, Acc) -> << Acc/binary, (EncodeFun(Elem))/binary >> end,
  lists:foldl(Fun, <<>>, List).


to_map([]) -> #{};
to_map(Map) ->
  to_map(Map, []).

to_map([A,B|T], Acc) ->
  to_map(T, [{A, B}|Acc]);
to_map([], Acc) ->
  maps:from_list(lists:reverse(Acc)).

index_of(Item, List) ->
  index_of(Item, List, 0).

index_of(_, [], _)  -> [];
index_of(Item, [Item|_], Index) -> Index;
index_of(Item, [_|Tl], Index) -> index_of(Item, Tl, Index + 1).

%%%===================================================================
%%% TESTS
%%%===================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(LONG_STR, <<
  "For encoded string containing fewer than 16 bytes, including empty strings, "
  "the marker byte should contain the high-order nibble `1000` followed by a "
  "low-order nibble containing the size. The encoded data then immediately "
  "follows the marker."
  "For encoded string containing 16 bytes or more, the marker 0xD0, 0xD1 or "
  "0xD2 should be used, depending on scale. This marker is followed by the"
  "size and the UTF-8 encoded data."
>>).

%% ebolt_packstream:decode(<<16#80>>).

%%%===================================================================
%% TESTS Decode
%%%===================================================================
decode_test_() ->
  Cases = [
    %% null
    {[null],    << 16#C0 >>},

    %% booleans
    {[true], << 16#C3 >>},
    {[false], << 16#C2 >>},

    %% floats
    {[1.1], << 16#C1, 16#3F, 16#F1, 16#99, 16#99, 16#99, 16#99, 16#99, 16#9A >>},
    {[-1.1], << 16#C1, 16#BF, 16#F1, 16#99, 16#99, 16#99, 16#99, 16#99, 16#9A >>},

    {[1], << 16#01 >>},
    {[-9223372036854775808], << 16#CB, 16#80, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00, 16#00 >>},
    %% max int
    {[9223372036854775807], << 16#CB, 16#7F, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF, 16#FF >>},
    %% any cases fo ints
    {[42], << 16#2A >>},
    {[42], << 16#C8, 16#2A >>},
    {[42], << 16#CB, 0, 0, 0, 0, 0, 0, 0, 16#2A>>},

    %% Strings
    {[<<>>], << 16#80 >>},
    {[<<"a">>], << 16#81, 16#61 >>},
    {[<<"abcdefghijklmnopqrstuvwxyz">>], << 16#D0, 16#1A, 16#61, 16#62, 16#63, 16#64, 16#65, 16#66, 16#67, 16#68, 16#69, 16#6A, 16#6B, 16#6C, 16#6D, 16#6E, 16#6F, 16#70, 16#71, 16#72, 16#73, 16#74, 16#75, 16#76, 16#77, 16#78, 16#79, 16#7A >>},
    {[<<"En å flöt över ängen"/utf8>>], << 16#D0, 16#18, 16#45, 16#6E, 16#20, 16#C3, 16#A5, 16#20, 16#66, 16#6C, 16#C3, 16#B6, 16#74, 16#20, 16#C3, 16#B6, 16#76, 16#65, 16#72, 16#20, 16#C3, 16#A4, 16#6E, 16#67, 16#65, 16#6E >>},

    %% Lists
    {[[]], << 16#90 >>},
    {[[1, 2, 3]], << 16#93, 16#01, 16#02, 16#03 >>},
    {[[1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6,7,8,9,0]], << 16#D4, 16#14, 16#01, 16#02, 16#03, 16#04, 16#05, 16#06, 16#07, 16#08, 16#09, 16#00, 16#01, 16#02, 16#03, 16#04, 16#05, 16#06, 16#07, 16#08, 16#09, 16#00 >>},
    {[[1,2,3]], <<16#D7, 16#01, 16#02, 16#03, 16#DF>>},           %% streaming case
    {[[<<"Julia">>]], <<145, 133, 74, 117, 108, 105, 97>>},

    %% Maps
    {[#{}], << 16#A0 >>},
    {[#{<<"a">> => 1}], << 16#A1, 16#81, 16#61, 16#01 >>},

    {[#{<<"a">> => 1}], << 16#DB, 16#81, 16#61, 16#01, 16#DF >>}, %% streaming case
    {[#{<<"a">> => 1, <<"b">> => 1, <<"c">> => 3, <<"d">> => 4, <<"e">> => 5,
        <<"f">> => 6, <<"g">> => 7, <<"h">> => 8, <<"i">> => 9, <<"j">> => 0,
        <<"k">> => 1, <<"l">> => 2, <<"m">> => 3, <<"n">> => 4, <<"o">> => 5, <<"p">> => 6}],
      <<  16#D8, 16#10, 16#81, 16#61, 16#01, 16#81, 16#62, 16#01, 16#81, 16#63, 16#03, 16#81, 16#64, 16#04,
          16#81, 16#65, 16#05, 16#81, 16#66, 16#06, 16#81, 16#67, 16#07, 16#81, 16#68, 16#08, 16#81,
          16#69, 16#09, 16#81, 16#6A, 16#00, 16#81, 16#6B, 16#01, 16#81, 16#6C, 16#02, 16#81, 16#6D, 16#03,
          16#81, 16#6E, 16#04, 16#81, 16#6F, 16#05, 16#81, 16#70, 16#06 >>
    },

    %% Structs
    {[[{sig,1}, {fields,[1,2,3]}]], << 16#B3, 16#01, 16#01, 16#02, 16#03 >>},
    {[[{sig, 1}, {fields, [1,2,3,4,5,6,7,8,9,0,1,2,3,4,5,6]}]],
      <<  16#DC, 16#10, 16#01, 16#01, 16#02, 16#03, 16#04, 16#05, 16#06, 16#07, 16#08, 16#09, 16#00, 16#01,
          16#02, 16#03, 16#04, 16#05, 16#06 >>
    }
  ],
  [?_assertEqual(Res, decode(In)) || {Res, In} <- Cases].

%% B = <<16#B1, 16#01, 16#8C, 16#4D, 16#79, 16#43, 16#6C, 16#69, 16#65, 16#6E, 16#74, 16#2F, 16#31, 16#2E, 16#30, 16#A3, 16#86, 16#73, 16#63, 16#68, 16#65, 16#6D, 16#65, 16#85, 16#62, 16#61, 16#73, 16#69, 16#63, 16#89, 16#70, 16#72, 16#69, 16#6E, 16#63, 16#69, 16#70, 16#61, 16#6C, 16#85, 16#6E, 16#65, 16#6F, 16#34, 16#6A, 16#8B, 16#63, 16#72, 16#65, 16#64, 16#65, 16#6E, 16#74, 16#69, 16#61, 16#6C, 16#73, 16#86, 16#73, 16#65, 16#63, 16#72, 16#65, 16#74 >>.
%%   ebolt_packstream:decode(B1).
%%
%% ebolt_packstream:encode(<<"basic">>).
%% ebolt_packstream:encode_map(#{<<"a">> => 1}, 1).
%%%===================================================================
%% TESTS Encode
%%%===================================================================
encode_test_() ->
  Cases = [
    %% null
    {<< 16#C0 >>, null},

    %% booleans
    {<< 16#C3 >>, true},
    {<< 16#C2 >>, false},

    %% floats
    {<< 16#C1, 16#3F, 16#F1, 16#99, 16#99, 16#99, 16#99, 16#99, 16#9A >>, 1.1},

    %% atoms
    {<< 16#85, 16#68, 16#65, 16#6C, 16#6C, 16#6F >>, hello},

    %% integers
    {<< 16#00 >>, 0},
    {<< 16#2A >>, 42},
    {<< 16#C8, 16#D6 >>, -42},
    {<< 16#C9, 16#01, 16#A4 >>, 420},

    %% Strings
    {<< 16#80 >>, <<>>},
    {<< 16#85, 16#53, 16#68, 16#6F, 16#72, 16#74>>, <<"Short">>},
    {<< 16#D0, 16#1A, 16#61, 16#62, 16#63, 16#64, 16#65, 16#66, 16#67, 16#68, 16#69, 16#6A, 16#6B, 16#6C, 16#6D, 16#6E, 16#6F, 16#70, 16#71, 16#72, 16#73, 16#74, 16#75, 16#76, 16#77, 16#78, 16#79, 16#7A >>, <<"abcdefghijklmnopqrstuvwxyz">>},
    {<< 16#D0, 16#18, 16#45, 16#6E, 16#20, 16#C3, 16#A5, 16#20, 16#66, 16#6C, 16#C3, 16#B6, 16#74, 16#20, 16#C3, 16#B6, 16#76, 16#65, 16#72, 16#20, 16#C3, 16#A4, 16#6E, 16#67, 16#65, 16#6E >>, <<"En å flöt över ängen"/utf8>>},

    %% Lists
    {<< 16#90 >>, []},
    {<< 16#93, 16#01, 16#02, 16#03 >>, [1,2,3]},

    %% Maps
    {<< 16#A0 >>, #{}},
    {<< 16#A1, 16#81, 16#61, 16#01 >>, #{<<"a">> => 1}}

],
  [?_assertEqual(Res, encode(In)) || {Res, In} <- Cases].


-endif.