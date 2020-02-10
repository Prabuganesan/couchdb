% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(fabric2_doc_size_tests).


-include_lib("couch/include/couch_db.hrl").
-include_lib("eunit/include/eunit.hrl").


% Doc body size calculations
% ID: size(Doc#doc.id)
% Rev: size(erlfdb_tuple:encode(Start)) + size(Rev) % where Rev is usually 16
% Deleted: 1 % (binary value is one byte)
% Body: couch_ejson_size:external_size(Body) % Where empty is {} which is 2)


-define(DOC_IDS, [
    {0, <<>>},
    {1, <<"a">>},
    {3, <<"foo">>},
    {6, <<"foobar">>},
    {32, <<"af196ae095631b020eedf8f69303e336">>}
]).

-define(REV_STARTS, [
    {1, 0},
    {2, 1},
    {2, 255},
    {3, 256},
    {3, 65535},
    {4, 65536},
    {4, 16777215},
    {5, 16777216},
    {5, 4294967295},
    {6, 4294967296},
    {6, 1099511627775},
    {7, 1099511627776},
    {7, 281474976710655},
    {8, 281474976710656},
    {8, 72057594037927935},
    {9, 72057594037927936},
    {9, 18446744073709551615},

    % The jump from 9 to 11 bytes is because when we
    % spill over into the bigint range of 9-255
    % bytes we have an extra byte that encodes the
    % length of the bigint.
    {11, 18446744073709551616}
]).

-define(REVS, [
    {0, <<>>},
    {8, <<"foobarba">>},
    {16, <<"foobarbazbambang">>}
]).

-define(DELETED, [
    {1, true},
    {1, false}
]).

-define(BODIES, [
    {2, {[]}},
    {13, {[{<<"foo">>, <<"bar">>}]}},
    {28, {[{<<"b">>, <<"a">>}, {<<"c">>, [true, null, []]}]}}
]).


empty_doc_test() ->
    ?assertEqual(4, fabric2_util:rev_size(#doc{})).


docid_size_test() ->
    lists:foreach(fun({Size, DocId}) ->
        ?assertEqual(4 + Size, fabric2_util:rev_size(#doc{id = DocId}))
    end, ?DOC_IDS).


rev_size_test() ->
    lists:foreach(fun({StartSize, Start}) ->
        lists:foreach(fun({RevSize, Rev}) ->
            Doc = #doc{
                revs = {Start, [Rev]}
            },
            ?assertEqual(3 + StartSize + RevSize, fabric2_util:rev_size(Doc))
        end, ?REVS)
    end, ?REV_STARTS).


deleted_size_test() ->
    lists:foreach(fun({Size, Deleted}) ->
        ?assertEqual(3 + Size, fabric2_util:rev_size(#doc{deleted = Deleted}))
    end, ?DELETED).


body_size_test() ->
    lists:foreach(fun({Size, Body}) ->
        ?assertEqual(2 + Size, fabric2_util:rev_size(#doc{body = Body}))
    end, ?BODIES).


combinatorics_test() ->
    Elements = [
        {?DOC_IDS, fun(Doc, DocId) -> Doc#doc{id = DocId} end},
        {?REV_STARTS, fun(Doc, RevStart) ->
            #doc{revs = {_, RevIds}} = Doc,
            Doc#doc{revs = {RevStart, RevIds}}
        end},
        {?REVS, fun(Doc, Rev) ->
           #doc{revs = {Start, _}} = Doc,
           Doc#doc{revs = {Start, [Rev]}}
        end},
        {?DELETED, fun(Doc, Deleted) -> Doc#doc{deleted = Deleted} end},
        {?BODIES, fun(Doc, Body) -> Doc#doc{body = Body} end}
    ],
    combine(Elements, 0, #doc{}).


combine([], TotalSize, Doc) ->
    ?assertEqual(TotalSize, fabric2_util:rev_size(Doc));

combine([{Elems, UpdateFun} | Rest], TotalSize, Doc) ->
    lists:foreach(fun({Size, Elem}) ->
        combine(Rest, TotalSize + Size, UpdateFun(Doc, Elem))
    end, Elems).
