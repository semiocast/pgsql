-module(pgsql_connection_test).
-include_lib("eunit/include/eunit.hrl").

%%%% CREATE ROLE test LOGIN;
%%%% ALTER USER test WITH SUPERUSER
%%%%
%%%% CREATE DATABASE test WITH OWNER=test;
%%%%

kill_sup(SupPid) ->
    OldTrapExit = process_flag(trap_exit, true),
    exit(SupPid, kill),
    receive {'EXIT', SupPid, _Reason} -> ok after 5000 -> throw({error, timeout}) end,
    process_flag(trap_exit, OldTrapExit).


open_close_test_() ->
    {setup,
    fun() ->
        {ok, Pid} = pgsql_connection_sup:start_link(),
        Pid
    end,
    fun(SupPid) ->
        kill_sup(SupPid)
    end,
    [
        {"Open connection to test database with test account",
        ?_test(begin
            R = pgsql_connection:open("test", "test"),
            pgsql_connection:close(R)
        end)},
        {"Open connection to test database with test account, expliciting empty password",
        ?_test(begin
            R = pgsql_connection:open("test", "test", ""),
            pgsql_connection:close(R)
        end)},
        {"Open connection to test database with test account, expliciting host",
        ?_test(begin
            R = pgsql_connection:open("0.0.0.0", "test", "test", ""),
            pgsql_connection:close(R)
        end)},
        {"Open connection to test database with test account, expliciting host, using IP for host and binaries for account/database/password",
        ?_test(begin
            R = pgsql_connection:open({0,0,0,0}, <<"test">>, <<"test">>, <<>>),
            pgsql_connection:close(R)
        end)},
        {"Open connection to test database with test account, expliciting host and options",
        ?_test(begin
            R = pgsql_connection:open("0.0.0.0", "test", "test", "", [{application_name, eunit_tests}]),
            pgsql_connection:close(R)
        end)},
        {"Open connection to test database with options as list",
        ?_test(begin
            R = pgsql_connection:open([{host, "0.0.0.0"}, {database, "test"}, {user, "test"}, {password, ""}]),
            pgsql_connection:close(R)
        end)},
        {"Bad user throws",
        ?_test(begin
            try
                R = pgsql_connection:open("test", "bad_user"),
                pgsql_connection:close(R),
                ?assert(false)
            catch throw:{pgsql_error, _Error} ->
                ok
            end
        end)}
    ]}.

select_null_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select null", Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select null", [], Conn)),
        ?_assertEqual({{select, 1}, [{null}]}, pgsql_connection:simple_query("select null", Conn)),
        ?_assertEqual({{select, 1}, [{null}]}, pgsql_connection:extended_query("select null", [], Conn))
    ]
    end}.

sql_query_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        {"Create temporary table",
            ?_assertEqual({updated, 1}, pgsql_connection:sql_query("create temporary table foo (id integer primary key, some_text text)", Conn))
        },
        {"Insert into",
            ?_assertEqual({updated, 1}, pgsql_connection:sql_query("insert into foo (id, some_text) values (1, 'hello')", Conn))
        },
        {"Update",
            ?_assertEqual({updated, 1}, pgsql_connection:sql_query("update foo set some_text = 'hello world'", Conn))
        },
        {"Insert into",
            ?_assertEqual({updated, 1}, pgsql_connection:sql_query("insert into foo (id, some_text) values (2, 'hello again')", Conn))
        },
        {"Update on matching condition",
            ?_assertEqual({updated, 1}, pgsql_connection:sql_query("update foo set some_text = 'hello world' where id = 1", Conn))
        },
        {"Update on non-matching condition",
            ?_assertEqual({updated, 0}, pgsql_connection:sql_query("update foo set some_text = 'goodbye, all' where id = 3", Conn))
        },
        {"Select *",
            ?_assertEqual({selected, [{1, <<"hello world">>}, {2, <<"hello again">>}]}, pgsql_connection:sql_query("select * from foo order by id asc", Conn))
        },
        {"Select with named columns",
            ?_assertEqual({selected, [{1, <<"hello world">>}, {2, <<"hello again">>}]}, pgsql_connection:sql_query("select id as the_id, some_text as the_text from foo order by id asc", Conn))
        },
        {"Select with inverted columns",
            ?_assertEqual({selected, [{<<"hello world">>, 1}, {<<"hello again">>, 2}]}, pgsql_connection:sql_query("select some_text, id from foo order by id asc", Conn))
        },
        {"Select with matching condition",
            ?_assertEqual({selected, [{<<"hello again">>}]}, pgsql_connection:sql_query("select some_text from foo where id = 2", Conn))
        },
        {"Select with non-matching condition",
            ?_assertEqual({selected, []}, pgsql_connection:sql_query("select * from foo where id = 3", Conn))
        }
    ]
    end}.

types_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        {"Create temporary table for the types",
            ?_assertEqual({updated, 1}, pgsql_connection:sql_query("create temporary table types (id integer primary key, an_integer integer, a_bigint bigint, a_text text, a_uuid uuid, a_bytea bytea, a_real real)", Conn))
        },
        {"Insert nulls (literal)",
            ?_assertEqual({updated, 1}, pgsql_connection:sql_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (1, null, null, null, null, null, null)", Conn))
        },
        {"Select nulls (1)",
            ?_assertMatch({selected, [{1, null, null, null, null, null, null}]}, pgsql_connection:sql_query("select * from types where id = 1", Conn))
        },
        {"Insert nulls (params)",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)", [2, null, null, null, null, null, null], Conn))
        },
        {"Select nulls (2)",
            ?_assertMatch({selected, [{2, null, null, null, null, null, null}]}, pgsql_connection:sql_query("select * from types where id = 2", Conn))
        },
        {"Insert integer",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [3, 42, null, null, null, null, null], Conn))
        },
        {"Insert bigint",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [4, null, 1099511627776, null, null, null, null], Conn))
        },
        {"Insert text (list)",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [5, null, null, "And in the end, the love you take is equal to the love you make", null, null, null], Conn))
        },
        {"Insert text (binary)",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [6, null, null, <<"And in the end, the love you take is equal to the love you make">>, null, null, null], Conn))
        },
        {"Insert uuid",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [7, null, null, null, <<"727F42A6-E6A0-4223-9B72-6A5EB7436AB5">>, null, null], Conn))
        },
        {"Insert bytea",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [8, null, null, null, null, <<"deadbeef">>, null], Conn))
        },
        {"Insert float",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [9, null, null, null, null, null, 3.1415], Conn))
        },
        {"Insert float",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [19, null, null, null, null, null, 3.0], Conn))
        },
        {"Insert all",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [10, 42, 1099511627776, "And in the end, the love you take is equal to the love you make", <<"727F42A6-E6A0-4223-9B72-6A5EB7436AB5">>, <<"deadbeef">>, 3.1415], Conn))
        },
        {"Select values (10)",
            ?_test(begin
                R = pgsql_connection:sql_query("select * from types where id = 10", Conn),
                ?assertMatch({selected, [_Row]}, R),
                {selected, [Row]} = R,
                ?assertMatch({10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, _UUID, <<"deadbeef">>, _Float}, Row),
                {10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, UUID, <<"deadbeef">>, Float} = Row,
                ?assertEqual(<<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>, UUID),
                ?assert(Float > 3.1413),
                ?assert(Float < 3.1416)
            end)
        },
        {"Select values (10) (with bind)",
            ?_test(begin
                R = pgsql_connection:param_query("select * from types where id = ?", [10], Conn),
                ?assertMatch({selected, [_Row]}, R),
                {selected, [Row]} = R,
                ?assertMatch({10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, _UUID, <<"deadbeef">>, _Float}, Row),
                {10, 42, 1099511627776, <<"And in the end, the love you take is equal to the love you make">>, UUID, <<"deadbeef">>, Float} = Row,
                ?assertEqual(<<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>, UUID),
                ?assert(Float > 3.1413),
                ?assert(Float < 3.1416)
            end)
        },
        {"Insert bytea",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [11, null, null, null, null, <<"deadbeef">>, null], Conn))
        },
        {"Insert with returning",
            ?_assertEqual({updated, 1, [{15}]}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?) RETURNING id",
                [15, null, null, null, null, <<"deadbeef">>, null], Conn))
        },
        {"Select values (11)",
            ?_test(begin
                R = pgsql_connection:param_query("select * from types where id = ?", [11], Conn),
                ?assertMatch({selected, [_Row]}, R),
                {selected, [Row]} = R,
                ?assertEqual({11, null, null, null, null, <<"deadbeef">>, null}, Row)
            end)
        },
        {"Insert uuid in lowercase",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [16, null, null, null, <<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>, null, null], Conn))
        },
        {"Insert uc uuid in text column",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [17, null, null, <<"727F42A6-E6A0-4223-9B72-6A5EB7436AB5">>, null, null, null], Conn))
        },
        {"Insert lc uuid in text column",
            ?_assertEqual({updated, 1}, pgsql_connection:param_query("insert into types (id, an_integer, a_bigint, a_text, a_uuid, a_bytea, a_real) values (?, ?, ?, ?, ?, ?, ?)",
                [18, null, null, <<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>, null, null, null], Conn))
        },
        {"Select text uuid (17 \& 18)",
            ?_test(begin
                R = pgsql_connection:param_query("select a_text from types where id IN ($1, $2) order by id", [17, 18], Conn),
                ?assertMatch({selected, [_Row17, _Row18]}, R),
                {selected, [Row17, Row18]} = R,
                ?assertEqual({<<"727F42A6-E6A0-4223-9B72-6A5EB7436AB5">>}, Row17),
                ?assertEqual({<<"727f42a6-e6a0-4223-9b72-6a5eb7436ab5">>}, Row18)
            end)
        }
        ]
    end}.

array_types_test_() ->
    {setup,
        fun() ->
                {ok, SupPid} = pgsql_connection_sup:start_link(),
                Conn = pgsql_connection:open("test", "test"),
                {SupPid, Conn}
        end,
        fun({SupPid, Conn}) ->
                pgsql_connection:close(Conn),
                kill_sup(SupPid)
        end,
        fun({_SupPid, Conn}) ->
                [
                    ?_assertEqual({{select,1},[{{array,[<<"2">>,<<"3">>]}}]}, pgsql_connection:simple_query("select '{2,3}'::text[]", Conn)),
                    ?_assertEqual({{select,1},[{{array,[2,3]}}]}, pgsql_connection:simple_query("select '{2,3}'::int[]", Conn)),
                    ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:simple_query("select '{}'::text[]", Conn)),
                    ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:simple_query("select '{}'::int[]", Conn)),
                    ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:simple_query("select ARRAY[]::text[]", Conn)),
                    ?_assertEqual({{select,1},[{{array,[<<"2">>,<<"3">>]}}]}, pgsql_connection:extended_query("select $1::text[]", ["{\"2\", \"3\"}"], Conn)),
                    ?_assertEqual({{select,1},[{{array,[<<"2">>,<<"3">>]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, ["2", "3"]}], Conn)),
                    ?_assertEqual({{select,1},[{{array,[<<"2">>,<<"3">>]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, [<<"2">>, <<"3">>]}], Conn)),
                    ?_assertEqual({{select,1},[{{array,[{array,[<<"2">>]},{array, [<<"3">>]}]}}]}, pgsql_connection:extended_query("select $1::text[]", [{array, [{array, [<<"2">>]}, {array, [<<"3">>]}]}], Conn)),
                    ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:extended_query("select '{}'::text[]", [], Conn)),
                    ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:extended_query("select '{}'::int[]", [], Conn)),
                    ?_assertEqual({{select,1},[{{array,[]}}]}, pgsql_connection:extended_query("select ARRAY[]::text[]", [], Conn)),
                    
                    ?_assertEqual({{select,1},[{{array,[{array,[<<"2">>]},{array, [<<"3">>]}]}}]}, pgsql_connection:simple_query("select '{{\"2\"}, {\"3\"}}'::text[][]", Conn)),
                    ?_assertEqual({{select,1},[{{array,[{array,[1,2]}, {array, [3,4]}]}}]}, pgsql_connection:simple_query("select ARRAY[ARRAY[1,2], ARRAY[3,4]]", Conn)),

                    ?_assertEqual({{select,1},[{{array,[1,2]}}]}, pgsql_connection:simple_query("select ARRAY[1,2]::list_of_int", Conn)),
                    ?_test(
                        begin
                                {updated, 1} = pgsql_connection:sql_query("create temporary table tmp (id integer primary key, ints integer[])", Conn),
                                Array = lists:seq(1,1000000),
                                R = pgsql_connection:extended_query("insert into tmp(id, ints) values($1, $2)", [1, {array, Array}], Conn),
                                ?assertEqual({{insert, 0, 1}, []}, R)
                        end)
                ]
        end
    }.

float_types_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_assertEqual({selected, [{1.0}]}, pgsql_connection:sql_query("select 1.0::float4", Conn)),
        ?_assertEqual({selected, [{1.0}]}, pgsql_connection:sql_query("select 1.0::float8", Conn)),
        ?_assertEqual({selected, [{1.0}]}, pgsql_connection:param_query("select 1.0::float4", [], Conn)),
        ?_assertEqual({selected, [{1.0}]}, pgsql_connection:param_query("select 1.0::float8", [], Conn)),

        ?_assertEqual({selected, [{3.14159}]}, pgsql_connection:sql_query("select 3.141592653589793::float4", Conn)),
        ?_assertEqual({selected, [{3.14159265358979}]}, pgsql_connection:sql_query("select 3.141592653589793::float8", Conn)),
        ?_assertEqual({selected, [{3.1415927410125732}]}, pgsql_connection:param_query("select 3.141592653589793::float4", [], Conn)),
        ?_assertEqual({selected, [{3.141592653589793}]}, pgsql_connection:param_query("select 3.141592653589793::float8", [], Conn)),

        ?_assertEqual({selected, [{'NaN'}]}, pgsql_connection:sql_query("select 'NaN'::float4", Conn)),
        ?_assertEqual({selected, [{'NaN'}]}, pgsql_connection:sql_query("select 'NaN'::float8", Conn)),
        ?_assertEqual({selected, [{'NaN'}]}, pgsql_connection:param_query("select 'NaN'::float4", [], Conn)),
        ?_assertEqual({selected, [{'NaN'}]}, pgsql_connection:param_query("select 'NaN'::float8", [], Conn)),

        ?_assertEqual({selected, [{'Infinity'}]}, pgsql_connection:sql_query("select 'Infinity'::float4", Conn)),
        ?_assertEqual({selected, [{'Infinity'}]}, pgsql_connection:sql_query("select 'Infinity'::float8", Conn)),
        ?_assertEqual({selected, [{'Infinity'}]}, pgsql_connection:param_query("select 'Infinity'::float4", [], Conn)),
        ?_assertEqual({selected, [{'Infinity'}]}, pgsql_connection:param_query("select 'Infinity'::float8", [], Conn)),

        ?_assertEqual({selected, [{'-Infinity'}]}, pgsql_connection:sql_query("select '-Infinity'::float4", Conn)),
        ?_assertEqual({selected, [{'-Infinity'}]}, pgsql_connection:sql_query("select '-Infinity'::float8", Conn)),
        ?_assertEqual({selected, [{'-Infinity'}]}, pgsql_connection:param_query("select '-Infinity'::float4", [], Conn)),
        ?_assertEqual({selected, [{'-Infinity'}]}, pgsql_connection:param_query("select '-Infinity'::float8", [], Conn))
    ]
    end}.

boolean_type_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_assertEqual({selected, [{true}]}, pgsql_connection:sql_query("select true::boolean", Conn)),
        ?_assertEqual({selected, [{false}]}, pgsql_connection:sql_query("select false::boolean", Conn)),
        ?_assertEqual({selected, [{true}]}, pgsql_connection:param_query("select true::boolean", [], Conn)),
        ?_assertEqual({selected, [{false}]}, pgsql_connection:param_query("select false::boolean", [], Conn))
    ]
    end}.

null_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select null", Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select null", [], Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select null::int2", Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select null::int2", [], Conn))
    ]
    end}.

integer_types_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_assertEqual({selected, [{127}]}, pgsql_connection:sql_query("select 127::int2", Conn)),
        ?_assertEqual({selected, [{-126}]}, pgsql_connection:sql_query("select -126::int2", Conn)),
        ?_assertEqual({selected, [{127}]}, pgsql_connection:sql_query("select 127::int4", Conn)),
        ?_assertEqual({selected, [{-126}]}, pgsql_connection:sql_query("select -126::int4", Conn)),
        ?_assertEqual({selected, [{127}]}, pgsql_connection:sql_query("select 127::int8", Conn)),
        ?_assertEqual({selected, [{-126}]}, pgsql_connection:sql_query("select -126::int8", Conn)),
        ?_assertEqual({selected, [{127}]}, pgsql_connection:param_query("select 127::int2", [], Conn)),
        ?_assertEqual({selected, [{-126}]}, pgsql_connection:param_query("select -126::int2", [], Conn)),
        ?_assertEqual({selected, [{127}]}, pgsql_connection:param_query("select 127::int4", [], Conn)),
        ?_assertEqual({selected, [{-126}]}, pgsql_connection:param_query("select -126::int4", [], Conn)),
        ?_assertEqual({selected, [{127}]}, pgsql_connection:param_query("select 127::int8", [], Conn)),
        ?_assertEqual({selected, [{-126}]}, pgsql_connection:param_query("select -126::int8", [], Conn))
    ]
    end}.

% Numerics can be either integers or floats.
numeric_types_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        % text values (simple_query)
        ?_assertEqual({{select, 1}, [{127}]}, pgsql_connection:simple_query("select 127::numeric", Conn)),
        ?_assertEqual({{select, 1}, [{-126}]}, pgsql_connection:simple_query("select -126::numeric", Conn)),
        ?_assertEqual({{select, 1}, [{123456789012345678901234567890}]}, pgsql_connection:simple_query("select 123456789012345678901234567890::numeric", Conn)),
        ?_assertEqual({{select, 1}, [{-123456789012345678901234567890}]}, pgsql_connection:simple_query("select -123456789012345678901234567890::numeric", Conn)),
        ?_assertEqual({{select, 1}, [{'NaN'}]}, pgsql_connection:simple_query("select 'NaN'::numeric", Conn)),
        ?_assertEqual({{select, 1}, [{123456789012345678901234.567890}]}, pgsql_connection:simple_query("select 123456789012345678901234.567890::numeric", Conn)),
        ?_assertEqual({{select, 1}, [{-123456789012345678901234.567890}]}, pgsql_connection:simple_query("select -123456789012345678901234.567890::numeric", Conn)),
        ?_assertEqual({{select, 1}, [{1000000.0}]}, pgsql_connection:simple_query("select 1000000.0::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{10000.0}]}, pgsql_connection:simple_query("select 10000.0::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{100.0}]}, pgsql_connection:simple_query("select 100.0::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{1.0}]}, pgsql_connection:simple_query("select 1.0::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{0.0}]}, pgsql_connection:simple_query("select 0.0::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{0.1}]}, pgsql_connection:simple_query("select 0.1::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{0.00001}]}, pgsql_connection:simple_query("select 0.00001::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{0.0000001}]}, pgsql_connection:simple_query("select 0.0000001::numeric", [], Conn)),

        % binary values (extended_query)
        ?_assertEqual({{select, 1}, [{127}]}, pgsql_connection:extended_query("select 127::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{-126}]}, pgsql_connection:extended_query("select -126::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{123456789012345678901234567890}]}, pgsql_connection:extended_query("select 123456789012345678901234567890::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{-123456789012345678901234567890}]}, pgsql_connection:extended_query("select -123456789012345678901234567890::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{'NaN'}]}, pgsql_connection:extended_query("select 'NaN'::numeric", [], Conn)),
        ?_test(begin
            {{select, 1}, [{Val}]} = pgsql_connection:extended_query("select 123456789012345678901234.567890::numeric", [], Conn),
            ?assert(Val > 123456789012345500000000.0),
            ?assert(Val < 123456789012345700000000.0)
        end),
        ?_test(begin
            {{select, 1}, [{Val}]} = pgsql_connection:extended_query("select -123456789012345678901234.567890::numeric", [], Conn),
            ?assert(Val > -123456789012345700000000.0),
            ?assert(Val < -123456789012345500000000.0)
        end),
        ?_assertEqual({{select, 1}, [{1000000.0}]}, pgsql_connection:extended_query("select 1000000.0::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{10000.0}]}, pgsql_connection:extended_query("select 10000.0::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{100.0}]}, pgsql_connection:extended_query("select 100.0::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{1.0}]}, pgsql_connection:extended_query("select 1.0::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{0.0}]}, pgsql_connection:extended_query("select 0.0::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{0.1}]}, pgsql_connection:extended_query("select 0.1::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{0.00001}]}, pgsql_connection:extended_query("select 0.00001::numeric", [], Conn)),
        ?_assertEqual({{select, 1}, [{0.0000001}]}, pgsql_connection:extended_query("select 0.0000001::numeric", [], Conn))
    ]
    end}.

datetime_types_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("127.0.0.1", "test", "test", "", [{timezone, "UTC"}]),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_assertEqual({selected, [{{2012,1,17}}]},    pgsql_connection:sql_query("select '2012-01-17 10:54:03.45'::date", Conn)),
        ?_assertEqual({selected, [{{10,54,3}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03'::time", Conn)),
        ?_assertEqual({selected, [{{10,54,3.45}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03.45'::time", Conn)),
        ?_assertEqual({selected, [{{10,54,3.45}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03.45'::timetz", Conn)),
        ?_assertEqual({selected, [{{{2012,1,17},{10,54,3}}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03'::timestamp", Conn)),
        ?_assertEqual({selected, [{{{2012,1,17},{10,54,3.45}}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03.45'::timestamp", Conn)),
        ?_assertEqual({selected, [{{{2012,1,17},{10,54,3.45}}}]},   pgsql_connection:sql_query("select '2012-01-17 10:54:03.45'::timestamptz", Conn)),
        ?_assertEqual({selected, [{{{1972,1,17},{10,54,3.45}}}]},   pgsql_connection:sql_query("select '1972-01-17 10:54:03.45'::timestamp", Conn)),
        ?_assertEqual({selected, [{{{1972,1,17},{10,54,3.45}}}]},   pgsql_connection:sql_query("select '1972-01-17 10:54:03.45'::timestamptz", Conn)),
        ?_assertEqual({selected, [{{1970,1,1}}]},   pgsql_connection:sql_query("select 'epoch'::date", Conn)),
        ?_assertEqual({selected, [{{0,0,0}}]},   pgsql_connection:sql_query("select 'allballs'::time", Conn)),
        ?_assertEqual({selected, [{infinity}]},   pgsql_connection:sql_query("select 'infinity'::timestamp", Conn)),
        ?_assertEqual({selected, [{'-infinity'}]},   pgsql_connection:sql_query("select '-infinity'::timestamp", Conn)),
        ?_assertEqual({selected, [{infinity}]},   pgsql_connection:sql_query("select 'infinity'::timestamptz", Conn)),
        ?_assertEqual({selected, [{'-infinity'}]},   pgsql_connection:sql_query("select '-infinity'::timestamptz", Conn)),

        ?_assertEqual({selected, [{{2012,1,17}}]},    pgsql_connection:param_query("select '2012-01-17 10:54:03.45'::date", [], Conn)),
        ?_assertEqual({selected, [{{10,54,3}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03'::time", [], Conn)),
        ?_assertEqual({selected, [{{10,54,3.45}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03.45'::time", [], Conn)),
        ?_assertEqual({selected, [{{10,54,3.45}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03.45'::timetz", [], Conn)),
        ?_assertEqual({selected, [{{{2012,1,17},{10,54,3}}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03'::timestamp", [], Conn)),
        ?_assertEqual({selected, [{{{2012,1,17},{10,54,3.45}}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03.45'::timestamp", [], Conn)),
        ?_assertEqual({selected, [{{{2012,1,17},{10,54,3.45}}}]},   pgsql_connection:param_query("select '2012-01-17 10:54:03.45'::timestamptz", [], Conn)),
        ?_assertEqual({selected, [{{{1972,1,17},{10,54,3.45}}}]},   pgsql_connection:param_query("select '1972-01-17 10:54:03.45'::timestamp", [], Conn)),
        ?_assertEqual({selected, [{{{1972,1,17},{10,54,3.45}}}]},   pgsql_connection:param_query("select '1972-01-17 10:54:03.45'::timestamptz", [], Conn)),
        ?_assertEqual({selected, [{{1970,1,1}}]},   pgsql_connection:param_query("select 'epoch'::date", [], Conn)),
        ?_assertEqual({selected, [{{0,0,0}}]},   pgsql_connection:param_query("select 'allballs'::time", [], Conn)),
        ?_assertEqual({selected, [{infinity}]},   pgsql_connection:param_query("select 'infinity'::timestamp", [], Conn)),
        ?_assertEqual({selected, [{'-infinity'}]},   pgsql_connection:param_query("select '-infinity'::timestamp", [], Conn)),
        ?_assertEqual({selected, [{infinity}]},   pgsql_connection:param_query("select 'infinity'::timestamptz", [], Conn)),
        ?_assertEqual({selected, [{'-infinity'}]},   pgsql_connection:param_query("select '-infinity'::timestamptz", [], Conn)),
        
        ?_assertEqual({{select, 1}, [{{{2012,1,17},{10,54,3}}}]},   pgsql_connection:extended_query("select $1::timestamptz", [{{2012,1,17},{10,54,3}}], Conn)),
        ?_assertEqual({{select, 1}, [{{2012,1,17}}]},   pgsql_connection:extended_query("select $1::date", [{2012,1,17}], Conn)),
        ?_assertEqual({{select, 1}, [{{10,54,3}}]},   pgsql_connection:extended_query("select $1::time", [{10,54,3}], Conn))
    ]
    end}.

fold_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        {timeout, 20,
        ?_test(begin
            {updated, 1} = pgsql_connection:sql_query("create temporary table tmp (id integer primary key, a_text text)", Conn),
            {updated, 0} = pgsql_connection:sql_query("BEGIN", Conn),
            Val = lists:foldl(fun(I, Acc) ->
                Str = "foobar " ++ integer_to_list(I * 42),
                {updated, 1} = pgsql_connection:param_query("insert into tmp (id, a_text) values (?, ?)", [I, Str], Conn),
                Acc + length(Str)
            end, 0, lists:seq(1, 3742)),
            {updated, 0} = pgsql_connection:sql_query("COMMIT", Conn),
            R = pgsql_connection:fold(fun({Text}, Acc) ->
                Acc + byte_size(Text)
            end, 0, "select a_text from tmp", Conn),
            ?assertEqual({ok, Val}, R)
        end)
        }
    ]
    end}.

map_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        {timeout, 20,
        ?_test(begin
            {updated, 1} = pgsql_connection:sql_query("create temporary table tmp (id integer primary key, a_text text)", Conn),
            {updated, 0} = pgsql_connection:sql_query("BEGIN", Conn),
            ValR = lists:foldl(fun(I, Acc) ->
                Str = "foobar " ++ integer_to_list(I * 42),
                {updated, 1} = pgsql_connection:param_query("insert into tmp (id, a_text) values (?, ?)", [I, Str], Conn),
                [length(Str) | Acc]
            end, [], lists:seq(1, 3742)),
            Val = lists:reverse(ValR),
            {updated, 0} = pgsql_connection:sql_query("COMMIT", Conn),
            R = pgsql_connection:map(fun({Text}) ->
                byte_size(Text)
            end, "select a_text from tmp", Conn),
            ?assertEqual({ok, Val}, R)
        end)
        }
    ]
    end}.

map_fold_foreach_should_return_when_query_is_invalid_test_() ->
   {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_test(begin
            R = pgsql_connection:extended_query("select toto", [], Conn),
            ?assertMatch({error, _}, R)
        end),
        ?_test(begin
            R = pgsql_connection:map(fun(_) -> ok end, "select toto", Conn),
            ?assertMatch({error, _}, R)
        end),
        ?_test(begin
            R = pgsql_connection:fold(fun(_,_) -> ok end, ok, "select toto", Conn),
            ?assertMatch({error, _}, R)
        end),
        ?_test(begin
            R = pgsql_connection:foreach(fun(_) -> ok end, "select toto", Conn),
            ?assertMatch({error, _}, R)
        end)
    ]
    end}.

foreach_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        {timeout, 20,
        ?_test(begin
            {updated, 1} = pgsql_connection:sql_query("create temporary table tmp (id integer primary key, a_text text)", Conn),
            {updated, 0} = pgsql_connection:sql_query("BEGIN", Conn),
            ValR = lists:foldl(fun(I, Acc) ->
                Str = "foobar " ++ integer_to_list(I * 42),
                {updated, 1} = pgsql_connection:param_query("insert into tmp (id, a_text) values (?, ?)", [I, Str], Conn),
                [length(Str) | Acc]
            end, [], lists:seq(1, 3742)),
            Val = lists:reverse(ValR),
            {updated, 0} = pgsql_connection:sql_query("COMMIT", Conn),
            Self = self(),
            R = pgsql_connection:foreach(fun({Text}) ->
                Self ! {foreach_inner, byte_size(Text)}
            end, "select a_text from tmp", Conn),
            ?assertEqual(ok, R),
            lists:foreach(fun(AVal) ->
                receive {foreach_inner, AVal} -> ok end
            end, Val)
        end)
        }
    ]
    end}.

timeout_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select pg_sleep(2)", Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select pg_sleep(2)", [], Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select pg_sleep(2)", [], infinity, Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select pg_sleep(2)", [], [], infinity, Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select pg_sleep(2)", [], 2500, Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select pg_sleep(2)", [], [], 2500, Conn)),
        ?_assertMatch({error, {pgsql_error, _}}, pgsql_connection:sql_query("select pg_sleep(2)", [], 1500, Conn)),
        ?_assertMatch({error, {pgsql_error, _}}, pgsql_connection:param_query("select pg_sleep(2)", [], [], 1500, Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select pg_sleep(2)", Conn)),
        ?_assertEqual({selected, [{null}]}, pgsql_connection:param_query("select pg_sleep(2)", [], Conn))
    ]
    end}.

ssl_test_OFF() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("127.0.0.1", "test", "test", "", [{ssl, true}]),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_assertEqual({selected, [{null}]}, pgsql_connection:sql_query("select null", Conn))
    ]
    end}.

constraint_violation_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_test(begin
            {updated, 1} = pgsql_connection:sql_query("create temporary table tmp (id integer primary key, a_text text)", Conn),
            {updated, 1} = pgsql_connection:param_query("insert into tmp (id, a_text) values (?, ?)", [1, <<"hello">>], Conn),
            E = pgsql_connection:param_query("insert into tmp (id, a_text) values (?, ?)", [1, <<"world">>], Conn),
            ?assertMatch({error, {pgsql_error, _}}, E),
            {error, Err} = E,
            ?assert(pgsql_error:is_integrity_constraint_violation(Err))
        end)
    ]
    end}.

custom_enum_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:sql_query("DROP TYPE mood;", Conn),
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_test(begin
            {updated, 0} = pgsql_connection:sql_query("BEGIN", Conn),
            {updated, 1} = pgsql_connection:sql_query("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');", Conn),
            ?assertMatch({selected, [{{MoodOID, <<"sad">>}}]} when is_integer(MoodOID), pgsql_connection:sql_query("select 'sad'::mood;", Conn)),
            ?assertMatch({selected, [{{MoodOID, <<"sad">>}}]} when is_integer(MoodOID), pgsql_connection:param_query("select 'sad'::mood;", [], Conn)),
            {updated, 0} = pgsql_connection:sql_query("COMMIT", Conn),
            ?assertMatch({selected, [{{mood, <<"sad">>}}]}, pgsql_connection:sql_query("select 'sad'::mood;", Conn)),
            ?assertMatch({selected, [{{mood, <<"sad">>}}]}, pgsql_connection:param_query("select 'sad'::mood;", [], Conn))
        end)
    ]
    end}.

custom_enum_native_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        {{drop, type}, []} = pgsql_connection:simple_query("DROP TYPE mood;", Conn),
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_test(begin
            {'begin', []} = pgsql_connection:simple_query("BEGIN", Conn),
            {{create, type}, []} = pgsql_connection:simple_query("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');", Conn),
            ?assertMatch({{select, 1}, [{{MoodOID, <<"sad">>}}]} when is_integer(MoodOID), pgsql_connection:simple_query("select 'sad'::mood;", Conn)),
            ?assertMatch({{select, 1}, [{{MoodOID, <<"sad">>}}]} when is_integer(MoodOID), pgsql_connection:extended_query("select 'sad'::mood;", [], Conn)),
            {'commit', []} = pgsql_connection:simple_query("COMMIT", Conn),
            ?assertMatch({{select, 1}, [{{mood, <<"sad">>}}]}, pgsql_connection:simple_query("select 'sad'::mood;", Conn)),
            ?assertMatch({{select, 1}, [{{mood, <<"sad">>}}]}, pgsql_connection:extended_query("select 'sad'::mood;", [], Conn))
        end)
    ]
    end}.

cancel_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_test(begin
            Self = self(),
            spawn_link(fun() ->
                SleepResult = pgsql_connection:sql_query("select pg_sleep(1)", Conn),
                Self ! {async_result, SleepResult}
            end),
            ?assertEqual(ok, pgsql_connection:cancel(Conn)),
            receive
                {async_result, R} ->
                    ?assertMatch({error, {pgsql_error, _}}, R),
                    {error, {pgsql_error, F}} = R,
                    {code, Code} = lists:keyfind(code, 1, F),
                    ?assertEqual(Code, <<"57014">>)
            end
        end)
    ]
    end}.

pending_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        {timeout, 10, ?_test(begin
            {{create, table}, []} = pgsql_connection:simple_query("CREATE TEMPORARY TABLE tmp(id integer primary key, other text)", Conn),
            Parent = self(),
            WorkerA = spawn(fun() ->
                R0 = pgsql_connection:simple_query("SELECT COUNT(*) FROM tmp", Conn),
                Parent ! {r0, R0},
                receive continue -> ok end,
                R2 = pgsql_connection:simple_query("SELECT pg_sleep(1), COUNT(*) FROM tmp", Conn),
                Parent ! {r2, R2},
                R4 = pgsql_connection:simple_query("SELECT pg_sleep(1), COUNT(*) FROM tmp", Conn),
                Parent ! {r4, R4},
                R6 = pgsql_connection:simple_query("SELECT COUNT(*) FROM tmp", Conn),
                Parent ! {r6, R6}
            end),
            spawn(fun() ->
                R1 = pgsql_connection:simple_query("INSERT INTO tmp (id) VALUES (1)", Conn),
                Parent ! {r1, R1},
                WorkerA ! continue,
                loop_until_process_is_waiting(WorkerA), % make sure command 2 was sent.
                R3 = pgsql_connection:simple_query("INSERT INTO tmp SELECT 2 AS id, CAST (pg_sleep(0.5) AS text) AS other", Conn),
                Parent ! {r3, R3},
                R5 = pgsql_connection:simple_query("INSERT INTO tmp (id) VALUES (3)", Conn),
                Parent ! {r5, R5}
            end),
            receive {RT0, R0} -> ?assertEqual(r0, RT0), ?assertEqual({{select, 1}, [{0}]}, R0) end,
            receive {RT1, R1} -> ?assertEqual(r1, RT1), ?assertEqual({{insert, 0, 1}, []}, R1) end,
            receive {RT2, R2} -> ?assertEqual(r2, RT2), ?assertEqual({{select, 1}, [{null, 1}]}, R2) end,
            receive {RT3, R3} -> ?assertEqual(r3, RT3), ?assertEqual({{insert, 0, 1}, []}, R3) end,
            receive {RT4, R4} -> ?assertEqual(r4, RT4), ?assertEqual({{select, 1}, [{null, 2}]}, R4) end,
            receive {RT5, R5} -> ?assertEqual(r5, RT5), ?assertEqual({{insert, 0, 1}, []}, R5) end,
            receive {RT6, R6} -> ?assertEqual(r6, RT6), ?assertEqual({{select, 1}, [{3}]}, R6) end
        end)}
    ]
    end}.

loop_until_process_is_waiting(Pid) ->
    case process_info(Pid, status) of
        {status, waiting} -> ok;
        _ -> loop_until_process_is_waiting(Pid)
    end.

batch_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        Conn = pgsql_connection:open("test", "test"),
        {SupPid, Conn}
    end,
    fun({SupPid, Conn}) ->
        pgsql_connection:close(Conn),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn}) ->
    [
        ?_assertEqual([{{select, 1}, [{1}]},{{select, 1}, [{2}]},{{select, 1}, [{3}]}], pgsql_connection:batch_query("select $1::int", [[1], [2], [3]], Conn)),
        ?_assertEqual([{{select, 1}, [{<<"bar">>}]},{{select, 1}, [{<<"foo">>}]},{{select, 1}, [{null}]}], pgsql_connection:batch_query("select $1::bytea", [[<<"bar">>], [<<"foo">>], [null]], Conn))
    ]
    end}.

async_process_loop(TestProcess) ->
    receive
        {set_test_process, Pid} ->
            async_process_loop(Pid);
        OtherMessage ->
            ?assert(is_pid(TestProcess)),
            TestProcess ! {self(), OtherMessage},
            async_process_loop(TestProcess)
    end.
        
notify_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        AsyncProcess = spawn_link(fun() ->
            async_process_loop(undefined)
        end),
        Conn1 = pgsql_connection:open([{database, "test"}, {user, "test"}, {async, AsyncProcess}]),
        Conn2 = pgsql_connection:open("test", "test"),
        {SupPid, Conn1, Conn2, AsyncProcess}
    end,
    fun({SupPid, Conn1, Conn2, AsyncProcess}) ->
        pgsql_connection:close(Conn1),
        pgsql_connection:close(Conn2),
        unlink(AsyncProcess),
        exit(AsyncProcess, normal),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn1, Conn2, AsyncProcess}) ->
    [
        ?_test(begin
            R = pgsql_connection:simple_query("LISTEN test_channel", Conn1),
            ?assertEqual({listen, []}, R)
        end),
        {"Notifications are received while idle",
        ?_test(begin
            AsyncProcess ! {set_test_process, self()},
            R = pgsql_connection:simple_query("NOTIFY test_channel", Conn2),
            ?assertEqual({notify, []}, R),
            receive {AsyncProcess, NotifyMessage} ->
                ?assertMatch({pgsql, Conn1, {notification, _PID, <<"test_channel">>, <<>>}}, NotifyMessage)
            after 1000 -> ?assert(false)
            end
        end)
        },
        {"Notifications are received with payload",
        ?_test(begin
            AsyncProcess ! {set_test_process, self()},
            R = pgsql_connection:simple_query("NOTIFY test_channel, 'payload string'", Conn2),
            ?assertEqual({notify, []}, R),
            receive {AsyncProcess, NotifyMessage} ->
                ?assertMatch({pgsql, Conn1, {notification, _PID, <<"test_channel">>, <<"payload string">>}}, NotifyMessage)
            after 1000 -> ?assert(false)
            end
        end)
        },
        {"Notifications are received with a busy connection executing several requests",
        ?_test(begin
            Parent = self(),
            AsyncProcess ! {set_test_process, Parent},
            spawn_link(fun() ->
                R = pgsql_connection:simple_query("SELECT pg_sleep(0.5)", Conn1),
                ?assertEqual({{select, 1}, [{null}]}, R),
                AsyncProcess ! sleep_1
            end),
            timer:sleep(100),
            spawn_link(fun() ->
                R = pgsql_connection:simple_query("SELECT pg_sleep(0.5)", Conn1),
                ?assertEqual({{select, 1}, [{null}]}, R),
                AsyncProcess ! sleep_2
            end),
            R = pgsql_connection:simple_query("NOTIFY test_channel", Conn2),
            ?assertEqual({notify, []}, R),
            % Acceptable orders are : sleep_1, notification, sleep_2 or notification, sleep_1, sleep_2.
            % PostgreSQL currently (9.2) sends notification after sleep_1 is completed, once the transaction is finished.
            % See note at http://www.postgresql.org/docs/9.2/static/protocol-flow.html#PROTOCOL-ASYNC
            Message0 = receive {AsyncProcess, Msg0} -> Msg0 after 1500 -> ?assert(false) end,
            Message1 = receive {AsyncProcess, Msg1} -> Msg1 after 1500 -> ?assert(false) end,
            Message2 = receive {AsyncProcess, Msg2} -> Msg2 after 1500 -> ?assert(false) end,
            ?assertEqual(sleep_2, Message2),
            case Message0 of
                sleep_1 ->
                    ?assertMatch({pgsql, Conn1, {notification, _PID, <<"test_channel">>, <<>>}}, Message1);
                {pgsql, Conn1, {notification, _PID, <<"test_channel">>, <<>>}} ->
                    ?assertEqual(sleep_1, Message1)
            end
        end)
        },
        {"Subscribe for notifications",
        ?_test(begin
            pgsql_connection:subscribe(self(), Conn1),
            AsyncProcess ! {set_test_process, self()},
            R = pgsql_connection:simple_query("NOTIFY test_channel, '1'", Conn2),
            ?assertEqual({notify, []}, R),
            receive {AsyncProcess, {pgsql, Conn1, {notification, _PID1, <<"test_channel">>, <<"1">>}}} -> ok
            after 1000 -> ?assert(false)
            end,
            receive {pgsql, Conn1, {notification, _PID2, <<"test_channel">>, <<"1">>}} -> ok
            after 1000 -> ?assert(false)
            end,
            pgsql_connection:unsubscribe(self(), Conn1),
            R = pgsql_connection:simple_query("NOTIFY test_channel, '2'", Conn2),
            ?assertEqual({notify, []}, R),
            receive {AsyncProcess, {pgsql, Conn1, {notification, _PID3, <<"test_channel">>, <<"2">>}}} -> ok
            after 1000 -> ?assert(false)
            end,
            receive {pgsql, Conn1, {notification, _PID4, <<"test_channel">>, <<"2">>}} -> ?assert(false)
            after 1000 -> ok
            end
        end)
        }
    ]
    end}.

notice_test_() ->
    {setup,
    fun() ->
        {ok, SupPid} = pgsql_connection_sup:start_link(),
        NoticeProcess = spawn_link(fun() ->
            async_process_loop(undefined)
        end),
        Conn1 = pgsql_connection:open([{database, "test"}, {user, "test"}, {async, NoticeProcess}]),
        {SupPid, Conn1, NoticeProcess}
    end,
    fun({SupPid, Conn1, NoticeProcess}) ->
        pgsql_connection:close(Conn1),
        unlink(NoticeProcess),
        exit(NoticeProcess, normal),
        kill_sup(SupPid)
    end,
    fun({_SupPid, Conn1, AsyncProcess}) ->
    [
        ?_test(begin
            AsyncProcess ! {set_test_process, self()},
            R = pgsql_connection:simple_query("DO $$ BEGIN RAISE NOTICE 'test notice'; END $$;", Conn1),
            ?assertEqual({'do', []}, R),
            receive {AsyncProcess, NoticeMessage} ->
                ?assertMatch({pgsql, Conn1, {notice, _Fields}}, NoticeMessage),
                {pgsql, Conn1, {notice, Fields}} = NoticeMessage,
                ?assertEqual({severity, <<"NOTICE">>}, lists:keyfind(severity, 1, Fields)),
                ?assertEqual({message, <<"test notice">>}, lists:keyfind(message, 1, Fields))
            after 1000 -> ?assert(false)
            end
        end)
    ]
    end}.

